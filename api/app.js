// Require Libraries

import cron from "node-cron";
import fetch from "node-fetch";
import dotenv from "dotenv";

import {
  FETCH_EXISTING_TICKERS,
  UPSERT_TS_ON_EXISTING_TICKERS,
  GRAPHQL_URL,
  setQueryOptions,
  getStockTimeSeriesAPI,
  transformStockAPIResponse,
} from "./helpers.js";

dotenv.config({ path: "../.env" });

// function to call Hasura and find existing tickers in time series database,

const existingTickerArray = (async () => {
  const result = await fetch(
    GRAPHQL_URL,
    setQueryOptions(FETCH_EXISTING_TICKERS)
  ).then(async (res) => {
    const result = await res.json();
    const tickerArray = result.data.asset_regular_ts.map((it) => it.ticker);
    return tickerArray;
  });
  return result;
})();

// loop through each ticker in DB, call API every 30 seconds, update new TS data

const upsertStockTimeSeriesOnDelay = () => {
  existingTickerArray.then((tickers) => {
    for (let i = 0; i < tickers.length; i++) {
      // delay loop by 30 seconds
      setTimeout(() => {
        console.log(new Date());
        console.log("now updating time series for:", tickers[i]);
        getStockTimeSeriesAPI(tickers[i]).then((res) => {
          // print first row of response data
          console.log(transformStockAPIResponse(res)[0]);
          fetch(
            GRAPHQL_URL,
            setQueryOptions(
              UPSERT_TS_ON_EXISTING_TICKERS,
              transformStockAPIResponse(res)
            )
          ).then(async (res) => {
            const result = await res.json();
            console.log("end result", result);
          });
        });
      }, 30000 * i);
    }
  });
};

upsertStockTimeSeriesOnDelay();
// run upsert function every day at midnight
cron.schedule("0 0 * * *", () => {
  console.log("cron is running");
  upsertStockTimeSeriesOnDelay();
});
