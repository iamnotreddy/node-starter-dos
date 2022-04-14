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

// call Hasura to find existing tickers in time series database,

const existingTickerArray = (async () => {
  const result = await fetch(
    GRAPHQL_URL,
    setQueryOptions(FETCH_EXISTING_TICKERS)
  ).then(async (res) => {
    const result = await res.json();
    const tickerArray = result.data.asset_regular_ts.map((it) => {
      return it.ticker;
    });
    return tickerArray;
  });
  return result;
})();

existingTickerArray.then((tickers) => {
  let tickerStep = 0;

  cron.schedule("*/30 * * * * *", () => {
    console.log("now updating time series for:", tickers[tickerStep]);
    getStockTimeSeriesAPI(tickers[tickerStep]).then((res) => {
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

    tickerStep += 1;
  });
});
