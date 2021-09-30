/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.TransactionHandlers;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities.Option;

namespace QuantConnect.Lean.Engine
{
    /// <summary>
    /// Helper class to handle delistings and splits for the <see cref="AlgorithmManager"/>
    /// </summary>
    public class SplitDelistingManager
    {
        private readonly IAlgorithm _algorithm;
        private readonly List<Delisting> _delistings = new();
        private readonly List<Split> _splitWarnings = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="SplitDelistingManager"/> class
        /// </summary>
        /// <param name="algorithm">The algorithm instance</param>
        public SplitDelistingManager(IAlgorithm algorithm)
        {
            _algorithm = algorithm;
        }

        /// <summary>
        /// Performs actual delisting of the contracts in delistings collection
        /// </summary>
        public void ProcessDelistedSymbols()
        {
            for (var i = _delistings.Count - 1; i >= 0; i--)
            {
                var delisting = _delistings[i];
                var security = _algorithm.Securities[delisting.Symbol];
                if (security.Holdings.Quantity == 0)
                {
                    continue;
                }

                if (security.LocalTime < delisting.GetLiquidationTime(security.Exchange.Hours))
                {
                    continue;
                }

                // if there is any delisting event for a symbol that we are the underlying for and we are still invested retry
                // they will by liquidated first
                if (_delistings.Any(delistingEvent => delistingEvent.Symbol.Underlying == security.Symbol
                    && _algorithm.Securities[delistingEvent.Symbol].Invested))
                {
                    // this case could happen for example if we have a future 'A' position open and a future option position with underlying 'A'
                    // and both get delisted on the same date, we will allow the FOP exercise order to get handled first
                    continue;
                }

                _delistings.RemoveAt(i);

                if (security.Symbol.SecurityType.IsOption() && !_algorithm.LiveMode)
                {
                    var option = (Option)security;

                    var orderProcessor = _algorithm.Transactions.GetOrderProcessor();
                    if (orderProcessor is BrokerageTransactionHandler transactionHandler)
                    {
                        transactionHandler.HandleOptionNotification(new OptionNotificationEventArgs(option.Symbol, 0));
                    }
                }
                else
                {
                    // submit an order to liquidate on market close or exercise (for options)
                    var request = security.CreateDelistedSecurityOrderRequest(_algorithm.UtcTime);

                    _algorithm.Transactions.ProcessRequest(request);
                }

                // don't allow users to open a new position once we sent the liquidation order
                security.IsTradable = false;
            }
        }

        /// <summary>
        /// Performs delisting logic for the securities specified in <paramref name="newDelistings"/> that are marked as <see cref="DelistingType.Delisted"/>.
        /// </summary>
        public void HandleDelistedSymbols(Delistings newDelistings)
        {
            foreach (var delisting in newDelistings.Values)
            {
                Log.Trace($"AlgorithmManager.HandleDelistedSymbols(): Delisting {delisting.Type}: {delisting.Symbol.Value}, UtcTime: {_algorithm.UtcTime}, DelistingTime: {delisting.Time}");

                if (_algorithm.LiveMode)
                {
                    // skip automatic handling of delisting event in live trading
                    // Lean will not exercise, liquidate or cancel open orders
                    continue;
                }

                // submit an order to liquidate on market close
                if (delisting.Type == DelistingType.Warning)
                {
                    if (_delistings.All(x => x.Symbol != delisting.Symbol))
                    {
                        _delistings.Add(delisting);
                    }
                }
                else
                {
                    // mark security as no longer tradable
                    var security = _algorithm.Securities[delisting.Symbol];
                    security.IsTradable = false;
                    security.IsDelisted = true;

                    // the subscription are getting removed from the data feed because they end
                    // remove security from all universes
                    foreach (var ukvp in _algorithm.UniverseManager)
                    {
                        var universe = ukvp.Value;
                        if (universe.ContainsMember(security.Symbol))
                        {
                            var userUniverse = universe as UserDefinedUniverse;
                            if (userUniverse != null)
                            {
                                userUniverse.Remove(security.Symbol);
                            }
                            else
                            {
                                universe.RemoveMember(_algorithm.UtcTime, security);
                            }
                        }
                    }

                    var cancelledOrders = _algorithm.Transactions.CancelOpenOrders(delisting.Symbol);
                    foreach (var cancelledOrder in cancelledOrders)
                    {
                        Log.Trace("AlgorithmManager.Run(): " + cancelledOrder);
                    }
                }
            }
        }

        /// <summary>
        /// Liquidate option contact holdings who's underlying security has split
        /// </summary>
        public void ProcessSplitSymbols()
        {
            // NOTE: This method assumes option contracts have the same core trading hours as their underlying contract
            //       This is a small performance optimization to prevent scanning every contract on every time step,
            //       instead we scan just the underlyings, thereby reducing the time footprint of this methods by a factor
            //       of N, the number of derivative subscriptions
            for (int i = _splitWarnings.Count - 1; i >= 0; i--)
            {
                var split = _splitWarnings[i];
                var security = _algorithm.Securities[split.Symbol];

                if (!security.IsTradable
                    && !_algorithm.UniverseManager.ActiveSecurities.Keys.Contains(split.Symbol))
                {
                    Log.Debug($"AlgorithmManager.ProcessSplitSymbols(): {_algorithm.Time} - Removing split warning for {security.Symbol}");

                    // remove the warning from out list
                    _splitWarnings.RemoveAt(i);
                    // Since we are storing the split warnings for a loop
                    // we need to check if the security was removed.
                    // When removed, it will be marked as non tradable but just in case
                    // we expect it not to be an active security either
                    continue;
                }

                var nextMarketClose = security.Exchange.Hours.GetNextMarketClose(security.LocalTime, false);

                // determine the latest possible time we can submit a MOC order
                var configs = _algorithm.SubscriptionManager.SubscriptionDataConfigService
                    .GetSubscriptionDataConfigs(security.Symbol);

                if (configs.Count == 0)
                {
                    // should never happen at this point, if it does let's give some extra info
                    throw new Exception(
                        $"AlgorithmManager.ProcessSplitSymbols(): {_algorithm.Time} - No subscriptions found for {security.Symbol}" +
                        $", IsTradable: {security.IsTradable}" +
                        $", Active: {_algorithm.UniverseManager.ActiveSecurities.Keys.Contains(split.Symbol)}");
                }

                var latestMarketOnCloseTimeRoundedDownByResolution = nextMarketClose.Subtract(MarketOnCloseOrder.SubmissionTimeBuffer)
                    .RoundDownInTimeZone(configs.GetHighestResolution().ToTimeSpan(), security.Exchange.TimeZone, configs.First().DataTimeZone);

                // we don't need to do anyhing until the market closes
                if (security.LocalTime < latestMarketOnCloseTimeRoundedDownByResolution) continue;

                // fetch all option derivatives of the underlying with holdings (excluding the canonical security)
                var derivatives = _algorithm.Securities.Where(kvp => kvp.Key.HasUnderlying &&
                    kvp.Key.SecurityType.IsOption() &&
                    kvp.Key.Underlying == security.Symbol &&
                    !kvp.Key.Underlying.IsCanonical() &&
                    kvp.Value.HoldStock
                );

                foreach (var kvp in derivatives)
                {
                    var optionContractSymbol = kvp.Key;
                    var optionContractSecurity = (Option)kvp.Value;

                    if (_delistings.Any(x => x.Symbol == optionContractSymbol
                        && x.Time.Date == optionContractSecurity.LocalTime.Date))
                    {
                        // if the option is going to be delisted today we skip sending the market on close order
                        continue;
                    }

                    // close any open orders
                    _algorithm.Transactions.CancelOpenOrders(optionContractSymbol, "Canceled due to impending split. Separate MarketOnClose order submitted to liquidate position.");

                    var request = new SubmitOrderRequest(OrderType.MarketOnClose, optionContractSecurity.Type, optionContractSymbol,
                        -optionContractSecurity.Holdings.Quantity, 0, 0, _algorithm.UtcTime,
                        "Liquidated due to impending split. Option splits are not currently supported."
                    );

                    // send MOC order to liquidate option contract holdings
                    _algorithm.Transactions.AddOrder(request);

                    // mark option contract as not tradable
                    optionContractSecurity.IsTradable = false;

                    _algorithm.Debug($"MarketOnClose order submitted for option contract '{optionContractSymbol}' due to impending {split.Symbol.Value} split event. "
                        + "Option splits are not currently supported.");
                }

                // remove the warning from out list
                _splitWarnings.RemoveAt(i);
            }
        }

        /// <summary>
        /// Keeps track of split warnings so we can later liquidate option contracts
        /// </summary>
        public void HandleSplitSymbols(Splits newSplits)
        {
            foreach (var split in newSplits.Values)
            {
                if (split.Type != SplitType.Warning)
                {
                    Log.Trace($"AlgorithmManager.HandleSplitSymbols(): {_algorithm.Time} - Security split occurred: Split Factor: {split} Reference Price: {split.ReferencePrice}");
                    continue;
                }

                Log.Trace($"AlgorithmManager.HandleSplitSymbols(): {_algorithm.Time} - Security split warning: {split}");

                if (!_splitWarnings.Any(x => x.Symbol == split.Symbol && x.Type == SplitType.Warning))
                {
                    _splitWarnings.Add(split);
                }
            }
        }
    }
}
