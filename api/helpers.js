import fetch from "node-fetch";

// --------------------------------------- helper file --------------------------------------------- \\

export const GRAPHQL_URL = "https://superb-fawn-16.hasura.app/v1/graphql";

export const FETCH_EXISTING_TICKERS = {
  queryString: `query fetchExistingTickerTS {
        asset_regular_ts(distinct_on: [ticker], order_by: [{ticker: asc}]) {
            ticker
        }
        }`,
  operationName: "fetchExistingTickerTS",
};

export const UPSERT_TS_ON_EXISTING_TICKERS = {
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

// fetches API response from Alpha Vantage and transforms into a list of objects

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

// functions to set Hasura API call headers, GQL query and input variables

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
