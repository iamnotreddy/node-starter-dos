// Require Libraries

import cron from "node-cron";
import fetch from "node-fetch";
import dotenv from "dotenv";
dotenv.config();

const GRAPHQL_URL = "https://superb-fawn-16.hasura.app/v1/graphql";

const FETCH_EXISTING_TICKERS = {
  queryString: `query fetchExistingTickerTS {
        asset_regular_ts(distinct_on: [ticker], order_by: [{ticker: asc}]) {
            ticker
        }
        }`,
  operationName: "fetchExistingTickerTS",
};

const UPSERT_TS_ON_EXISTING_TICKERS = {
  queryString: `mutation upsertTimeSeries ($objects: [asset_regular_ts_insert_input!]!) {
    insert_asset_regular_ts(objects: $objects,
      on_conflict: {
        constraint: asset_regular_ts_date_ticker_key,
        update_columns: []
      }
    ) {
      affected_rows
      returning {
        date,
        ticker,
        close_price
      }
    }
  }
  `,
  operationName: "upsertTimeSeries",
};

// helper functions

export const getStockTimeSeriesAPI = async (stockTicker) => {
  const res = await fetch(
    `https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=${stockTicker}&outputsize=full&apikey=${process.env.ALPHA_VANTAGE_API_KEY}`
  );
  const responseStock = await res.json();
  return responseStock;
};

export const transformStockAPIResponse = (response) => {
  const ticker = response["Meta Data"]["2. Symbol"];
  const keys = Object.keys(response["Time Series (Daily)"]);

  return keys.map((dataKey) => {
    return {
      date: new Date(dataKey),
      ticker: ticker,
      open_price: parseInt(response["Time Series (Daily)"][dataKey]["1. open"]),
      high_price: parseInt(response["Time Series (Daily)"][dataKey]["2. high"]),
      low_price: parseInt(response["Time Series (Daily)"][dataKey]["3. low"]),
      close_price: parseInt(
        response["Time Series (Daily)"][dataKey]["4. close"]
      ),
      volume: parseInt(response["Time Series (Daily)"][dataKey]["5. volume"]),
    };
  });
};

export const setQueryOptions = (queryObject, transformedObjectArray) => {
  if (transformedObjectArray === undefined) {
    const options = {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "x-hasura-admin-secret": process.env.NEXT_PUBLIC_HASURA_ADMIN_SECRET,
      },

      body: JSON.stringify({
        operationName: queryObject.operationName,
        query: queryObject.queryString,
      }),
    };
    return options;
  } else {
    const options = {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "x-hasura-admin-secret": process.env.NEXT_PUBLIC_HASURA_ADMIN_SECRET,
      },

      body: JSON.stringify({
        operationName: queryObject.operationName,
        query: queryObject.queryString,
        variables: { objects: transformedObjectArray },
      }),
    };

    return options;
  }
};

// actual tasks

// const existingTickerArray = (async () => {
//   const result = await fetch(
//     GRAPHQL_URL,
//     setQueryOptions(FETCH_EXISTING_TICKERS)
//   ).then(async (res) => {
//     const result = await res.json();
//     const tickerArray = result.data.asset_regular_ts.map((it) => {
//       return it.ticker;
//     });
//     return tickerArray;
//   });
//   return result;
// })();

// existingTickerArray.then((tickers) => {
//   let tickerStep = 0;

//   cron.schedule("*/30 * * * * *", () => {
//     console.log("now updating time series for:", tickers[tickerStep]);
//     getStockTimeSeriesAPI(tickers[tickerStep]).then((res) => {
//       const transformedStockResponse = transformStockAPIResponse(res);
//       console.log('this is what i pass into objects', transformedStockResponse);
//       fetch(
//         GRAPHQL_URL,
//         setQueryOptions(UPSERT_TS_ON_EXISTING_TICKERS, transformedStockResponse)
//       ).then(async (res) => {
//         const result = await res.json();
//         console.log(result);
//       });
//     });
//     tickerStep += 1;
//   });
// });

getStockTimeSeriesAPI("AMZN").then((res) => {
  console.log("raveen reddy", res);
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

// Routes

// Start Server
