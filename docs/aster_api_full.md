

---

# README.md

# API Documentation for Aster Finance Futures

[Aster Finance Futures API Document](./aster-finance-futures-api.md)

# Aster Finance Futures API 文档

[Aster Finance Futures API 文档](./aster-finance-futures-api_CN.md)

# API V3 Documentation for Aster Finance Futures

[Aster Finance Futures API V3 Document](./aster-finance-futures-api-v3.md)

# Aster Finance Futures API V3 文档

[Aster Finance Futures API V3 文档](./aster-finance-futures-api-v3_CN.md)

# Aster Finance Futures API V3 DEMO

[Aster Finance Futures API V3 DEMO](./v3-demo/tx.py)

# API Documentation for Aster Finance Spot

[Aster Finance Spot API Document](./aster-finance-spot-api.md)

# Aster Finance Spot API 文档

[Aster Finance Spot API 文档](./aster-finance-spot-api_CN.md)

# Aster Finance Spot Asset Consolidation

[Aster Finance Spot Asset Consolidation](./consolidation.js)

# Aster 现货资产归集脚本示例

[Aster 现货资产归集脚本示例](./consolidation.js)


---

# aster-finance-futures-api-v3.md

> > - [General Info](#general-info)

- [General API Information](#general-api-information)
  - [HTTP Return Codes](#http-return-codes)
  - [Error Codes and Messages](#error-codes-and-messages)
  - [General Information on Endpoints](#general-information-on-endpoints)
- [LIMITS](#limits)
  - [IP Limits](#ip-limits)
  - [Order Rate Limits](#order-rate-limits)
- [API authentication type](#api-authentication-type)
- [Authentication signature payload](#authentication-signature-payload)
- [Timing Security](#timing-security)
- [Endpoints requiring signature](#endpoints-requiring-signature)
  - [POST /fapi/v3/order example](#example-of-post-fapiv3order)
  - [GET /fapi/v3/order example](#Eexample-of-get-fapiv3order)
  - [python script](#python-script)
- [Public Endpoints Info](#public-endpoints-info)
  - [Terminology](#terminology)
  - [ENUM definitions](#enum-definitions)
- [Filters](#filters)
  - [Symbol filters](#symbol-filters)
    - [PRICE_FILTER](#price_filter)
    - [LOT_SIZE](#lot_size)
    - [MARKET_LOT_SIZE](#market_lot_size)
    - [MAX_NUM_ORDERS](#max_num_orders)
    - [MAX_NUM_ALGO_ORDERS](#max_num_algo_orders)
    - [PERCENT_PRICE](#percent_price)
    - [MIN_NOTIONAL](#min_notional)
- [Market Data Endpoints](#market-data-endpoints)
  - [Test Connectivity](#test-connectivity)
  - [Check Server Time](#check-server-time)
  - [Exchange Information](#exchange-information)
  - [Order Book](#order-book)
  - [Recent Trades List](#recent-trades-list)
  - [Old Trades Lookup (MARKET_DATA)](#old-trades-lookup-market_data)
  - [Compressed/Aggregate Trades List](#compressedaggregate-trades-list)
  - [Kline/Candlestick Data](#klinecandlestick-data)
  - [Index Price Kline/Candlestick Data](#index-price-klinecandlestick-data)
  - [Mark Price Kline/Candlestick Data](#mark-price-klinecandlestick-data)
  - [Mark Price](#mark-price)
  - [Get Funding Rate History](#get-funding-rate-history)
  - [24hr Ticker Price Change Statistics](#24hr-ticker-price-change-statistics)
  - [Symbol Price Ticker](#symbol-price-ticker)
  - [Symbol Order Book Ticker](#symbol-order-book-ticker)
- [Websocket Market Streams](#websocket-market-streams)
  - [Live Subscribing/Unsubscribing to streams](#live-subscribingunsubscribing-to-streams)
    - [Subscribe to a stream](#subscribe-to-a-stream)
    - [Unsubscribe to a stream](#unsubscribe-to-a-stream)
    - [Listing Subscriptions](#listing-subscriptions)
    - [Setting Properties](#setting-properties)
    - [Retrieving Properties](#retrieving-properties)
    - [Error Messages](#error-messages)
  - [Aggregate Trade Streams](#aggregate-trade-streams)
  - [Mark Price Stream](#mark-price-stream)
  - [Mark Price Stream for All market](#mark-price-stream-for-all-market)
  - [Kline/Candlestick Streams](#klinecandlestick-streams)
  - [Individual Symbol Mini Ticker Stream](#individual-symbol-mini-ticker-stream)
  - [All Market Mini Tickers Stream](#all-market-mini-tickers-stream)
  - [Individual Symbol Ticker Streams](#individual-symbol-ticker-streams)
  - [All Market Tickers Streams](#all-market-tickers-streams)
  - [Individual Symbol Book Ticker Streams](#individual-symbol-book-ticker-streams)
  - [All Book Tickers Stream](#all-book-tickers-stream)
  - [Liquidation Order Streams](#liquidation-order-streams)
  - [All Market Liquidation Order Streams](#all-market-liquidation-order-streams)
  - [Partial Book Depth Streams](#partial-book-depth-streams)
  - [Diff. Book Depth Streams](#diff-book-depth-streams)
  - [How to manage a local order book correctly](#how-to-manage-a-local-order-book-correctly)
- [Account/Trades Endpoints](#accounttrades-endpoints)
  - [Change Position Mode(TRADE)](#change-position-modetrade)
  - [Get Current Position Mode(USER_DATA)](#get-current-position-modeuser_data)
  - [Change Multi-Assets Mode (TRADE)](#change-multi-assets-mode-trade)
  - [Get Current Multi-Assets Mode (USER_DATA)](#get-current-multi-assets-mode-user_data)
  - [New Order  (TRADE)](#new-order--trade)
  - [Place Multiple Orders  (TRADE)](#place-multiple-orders--trade)
  - [Transfer Between Futures And Spot (USER_DATA)](#transfer-between-futures-and-spot-user_data)
  - [Query Order (USER_DATA)](#query-order-user_data)
  - [Cancel Order (TRADE)](#cancel-order-trade)
  - [Cancel All Open Orders (TRADE)](#cancel-all-open-orders-trade)
  - [Cancel Multiple Orders (TRADE)](#cancel-multiple-orders-trade)
  - [Auto-Cancel All Open Orders (TRADE)](#auto-cancel-all-open-orders-trade)
  - [Query Current Open Order (USER_DATA)](#query-current-open-order-user_data)
  - [Current All Open Orders (USER_DATA)](#current-all-open-orders-user_data)
  - [All Orders (USER_DATA)](#all-orders-user_data)
  - [Futures Account Balance v3 (USER_DATA)](#futures-account-balance-v3-user_data)
  - [Account Information v3 (USER_DATA)](#account-information-v3-user_data)
  - [Change Initial Leverage (TRADE)](#change-initial-leverage-trade)
  - [Change Margin Type (TRADE)](#change-margin-type-trade)
  - [Modify Isolated Position Margin (TRADE)](#modify-isolated-position-margin-trade)
  - [Get Position Margin Change History (TRADE)](#get-position-margin-change-history-trade)
  - [Position Information v3 (USER_DATA)](#position-information-v3-user_data)
  - [Account Trade List (USER_DATA)](#account-trade-list-user_data)
  - [Get Income History(USER_DATA)](#get-income-historyuser_data)
  - [Notional and Leverage Brackets (USER_DATA)](#notional-and-leverage-brackets-user_data)
  - [Position ADL Quantile Estimation (USER_DATA)](#position-adl-quantile-estimation-user_data)
  - [User's Force Orders (USER_DATA)](#users-force-orders-user_data)
  - [User Commission Rate (USER_DATA)](#user-commission-rate-user_data)
- [User Data Streams](#user-data-streams)
  - [Start User Data Stream (USER_STREAM)](#start-user-data-stream-user_stream)
  - [Keepalive User Data Stream (USER_STREAM)](#keepalive-user-data-stream-user_stream)
  - [Close User Data Stream (USER_STREAM)](#close-user-data-stream-user_stream)
  - [Event: User Data Stream Expired](#event-user-data-stream-expired)
  - [Event: Margin Call](#event-margin-call)
  - [Event: Balance and Position Update](#event-balance-and-position-update)
  - [Event: Order Update](#event-order-update)
  - [Event: Account Configuration Update previous Leverage Update](#event-account-configuration-update-previous-leverage-update)
- [Error Codes](#error-codes)
  - [10xx - General Server or Network issues](#10xx---general-server-or-network-issues)
  - [11xx - Request issues](#11xx---request-issues)
  - [20xx - Processing Issues](#20xx---processing-issues)
  - [40xx - Filters and other Issues](#40xx---filters-and-other-issues)

# General Info

## General API Information

* This API may require the user's AGENT. To learn how to create an AGENT, please refer toSome endpoints will require an API Key. Please refer to [this page](https://www.asterdex.com/)
* The base endpoint is: **https://fapi.asterdex.com**
* All endpoints return either a JSON object or array.
* Data is returned in **ascending** order. Oldest first, newest last.
* All time and timestamp related fields are in milliseconds.
* All data types adopt definition in JAVA.

### HTTP Return Codes

* HTTP `4XX` return codes are used for for malformed requests;
  the issue is on the sender's side.
* HTTP `403` return code is used when the WAF Limit (Web Application Firewall) has been violated.
* HTTP `429` return code is used when breaking a request rate limit.
* HTTP `418` return code is used when an IP has been auto-banned for continuing to send requests after receiving `429` codes.
* HTTP `5XX` return codes are used for internal errors; the issue is on
  Aster's side.
* HTTP `503` return code is used when the API successfully sent the message but not get a response within the timeout period.
  It is important to **NOT** treat this as a failure operation; the execution status is
  **UNKNOWN** and could have been a success.

### Error Codes and Messages

* Any endpoint can return an ERROR

> ***The error payload is as follows:***

```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

* Specific error codes and messages defined in [Error Codes](#error-codes).

### General Information on Endpoints

* For `GET` endpoints, parameters must be sent as a `query string`.
* For POST, PUT, and DELETE method APIs, send data in the request body (content type application/x-www-form-urlencoded)
* Parameters may be sent in any order.

## LIMITS

* The `/fapi/v3/exchangeInfo` `rateLimits` array contains objects related to the exchange's `RAW_REQUEST`, `REQUEST_WEIGHT`, and `ORDER` rate limits. These are further defined in the `ENUM definitions` section under `Rate limiters (rateLimitType)`.
* A `429` will be returned when either rate limit is violated.

<aside class="notice">
Aster Finance has the right to further tighten the rate limits on users with intent to attack.
</aside>

### IP Limits

* Every request will contain `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` in the response headers which has the current used weight for the IP for all request rate limiters defined.
* Each route has a `weight` which determines for the number of requests each endpoint counts for. Heavier endpoints and endpoints that do operations on multiple symbols will have a heavier `weight`.
* When a 429 is received, it's your obligation as an API to back off and not spam the API.
* **Repeatedly violating rate limits and/or failing to back off after receiving 429s will result in an automated IP ban (HTTP status 418).**
* IP bans are tracked and **scale in duration** for repeat offenders, **from 2 minutes to 3 days**.
* **The limits on the API are based on the IPs, not the API keys.**

<aside class="notice">
It is strongly recommended to use websocket stream for getting data as much as possible, which can not only ensure the timeliness of the message, but also reduce the access restriction pressure caused by the request.
</aside>

### Order Rate Limits

* Every order response will contain a `X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)` header which has the current order count for the account for all order rate limiters defined.
* Rejected/unsuccessful orders are not guaranteed to have `X-MBX-ORDER-COUNT-**` headers in the response.
* **The order rate limit is counted against each account**.

**Serious trading is about timing.** Networks can be unstable and unreliable,
which can lead to requests taking varying amounts of time to reach the
servers. With `recvWindow`, you can specify that the request must be
processed within a certain number of milliseconds or be rejected by the
server.

<aside class="notice">
It is recommended to use a small recvWindow of 5000 or less!
</aside>

## API authentication type

* Each API has its own authentication type, which determines what kind of authentication is required when accessing it.
* If authentication is required, a signer should be included in the request body.

| Security Type | Description                               |
| ------------- | ----------------------------------------- |
| NONE          | API that does not require authentication  |
| TRADE         | A valid signer and signature are required |
| USER_DATA     | A valid signer and signature are required |
| USER_STREAM   | A valid signer and signature are required |
| MARKET_DATA   | A valid signer and signature are required |

## Authentication signature payload

| Parameter | Description                        |
| --------- | ---------------------------------- |
| user      | Main account wallet address        |
| signer    | API wallet address                 |
| nonce     | Current timestamp, in microseconds |
| signature | Signature                          |

## Endpoints requiring signature 
* Security Type: TRADE, USER_DATA, USER_STREAM, MARKET_DATA
* After converting the API parameters to strings, sort them by their key values in ASCII order to generate the final string. Note: All parameter values must be treated as strings during the signing process.
* After generating the string, combine it with the authentication signature parameters user, signer, and nonce, then use Web3’s ABI parameter encoding to generate the bytecode.
* After generating the bytecode, use the Keccak algorithm to generate the hash.
* Use the private key of **API wallet address** to sign the hash using web3’s ECDSA signature algorithm, generating the final signature.

### Timing Security

* A `SIGNED` endpoint also requires a parameter, `timestamp`, to be sent which
  should be the millisecond timestamp of when the request was created and sent.
* An additional parameter, `recvWindow`, may be sent to specify the number of
  milliseconds after `timestamp` the request is valid for. If `recvWindow`
  is not sent, **it defaults to 5000**.

> The logic is as follows:

```javascript
if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow){
    // process request
  } 
  else {
    // reject request
  }
```

## Example of POST /fapi/v3/order

#### All parameters are passed through the request body (Python 3.9.6)

#### The following parameters are API registration details. The values for user, signer, and privateKey are for demonstration purposes only (the privateKey corresponds to the signer).

| Key        | Value                                                              |
| ---------- | ------------------------------------------------------------------ |
| user       | 0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e                         |
| signer     | 0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0                         |
| privateKey | 0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1 |

#### The nonce parameter is the current system time in microseconds. If it exceeds the system time or lags behind it by more than 5 seconds, the request is considered invalid.

```python
#python
nonce = math.trunc(time.time()*1000000)
print(nonce)
#1748310859508867
```

```java
//java
Instant now = Instant.now();
long microsecond = now.getEpochSecond() * 1000000 + now.getNano() / 1000;
```
#### Example: The following parameters are business request parameters.

```python
my_dict = {'symbol': 'SANDUSDT', 'positionSide': 'BOTH', 'type': 'LIMIT', 'side': 'BUY',
	         'timeInForce': 'GTC', 'quantity': "190", 'price': 0.28694}
```

#### Example: All parameters are sent via form body (using Python as an example).

> **Step 1: Convert all business parameters to strings, then sort them in ASCII order to generate a string.**

```python
    #Define all element values as strings.
    def _trim_dict(my_dict) :
      for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

    #Remove elements with null (empty) values.
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    # my_dict['timestamp'] = 1749545309665
    #Convert elements to strings.
    _trim_dict(my_dict)
    #Generate a string sorted by ASCII values and remove special characters.
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    #{"positionSide":"BOTH","price":"0.28694","quantity":"190","recvWindow":"50000","side":"BUY","symbol":"SANDUSDT","timeInForce":"GTC","timestamp":"1749545309665","type":"LIMIT"}
```

> **Step 2: Take the string generated in Step 1 and encode it together with the account information and nonce using ABI encoding to generate a hash string.**

```python
   from eth_abi import encode
   from web3 import Web3
   #Use Web3 ABI to encode the generated string along with user, signer, and nonce.
   encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
   print(encoded.hex())
   #000000000000000000000000000000000000000000000000000000000000008000000000000000000000000063dd5acc6b1aa0f563956c0e534dd30b6dcf7c4e00000000000000000000000021cf8ae13bb72632562c6fff438652ba1a151bb00000000000000000000000000000000000000000000000000006361457bcec8300000000000000000000000000000000000000000000000000000000000000af7b22706f736974696f6e53696465223a22424f5448222c227072696365223a22302e3238363934222c227175616e74697479223a22313930222c227265637657696e646f77223a223530303030222c2273696465223a22425559222c2273796d626f6c223a2253414e4455534454222c2274696d65496e466f726365223a22475443222c2274696d657374616d70223a2231373439353435333039363635222c2274797065223a224c494d4954227d0000000000000000000000000000000000
   #keccak hex
   keccak_hex =Web3.keccak(encoded).hex()
   print(keccak_hex)
   #9e0273fc91323f5cdbcb00c358be3dee2854afb2d3e4c68497364a2f27a377fc
```

> **Step 3: Sign the hash generated in Step 2 using the privateKey.**

```python
    from eth_account import Account
    from eth_abi import encode
    from web3 import Web3, EthereumTesterProvider
    from eth_account.messages import encode_defunct

    signable_msg = encode_defunct(hexstr=keccak_hex)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    signature =  '0x'+signed_message.signature.hex()
    print(signature)
    #0x0337dd720a21543b80ff861cd3c26646b75b3a6a4b5d45805d4c1d6ad6fc33e65f0722778dd97525466560c69fbddbe6874eb4ed6f5fa7e576e486d9b5da67f31b
```

> **Step 4: Combine all parameters along with the signature generated in Step 3 into the request body.**

```python
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()
    url ='https://fapi.asterdex.com/fapi/v3/order'
    headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
    }
    res = requests.post(url,data=my_dict,headers=headers)
    print(url)
    #curl  -X POST 'https://fapi.asterdex.com/fapi/v3/order' -d 'symbol=SANDUSDT&positionSide=BOTH&type=LIMIT&side=BUY&timeInForce=GTC&quantity=190&price=0.28694&recvWindow=50000&timestamp=1749545309665&nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0&signature=0x0337dd720a21543b80ff861cd3c26646b75b3a6a4b5d45805d4c1d6ad6fc33e65f0722778dd97525466560c69fbddbe6874eb4ed6f5fa7e576e486d9b5da67f31b'
```

## Example of POST /fapi/v3/order

#### Example: All parameters are sent through the query string (Python 3.9.6).

#### Example: The following parameters are API registration information. user, signer, and privateKey are for demonstration purposes only (where privateKey is the private key of the agent).

| Key        | Value                                                              |
| ---------- | ------------------------------------------------------------------ |
| user       | 0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e                         |
| signer     | 0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0                         |
| privateKey | 0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1 |

#### Example: The nonce parameter should be the current system timestamp in microseconds. If it exceeds the current time or lags behind the system time by more than 5 seconds, it will be considered an invalid request.

```python
#python
nonce = math.trunc(time.time()*1000000)
print(nonce)
#1748310859508867
```

```java
//java
Instant now = Instant.now();
long microsecond = now.getEpochSecond() * 1000000 + now.getNano() / 1000;
```

#### Example: The following parameters are business request parameters.

```python
my_dict = {'symbol':'SANDUSDT','side':"SELL","type":'LIMIT','orderId':2194215}
```

> **Step 1: Convert all business parameters to strings and generate a sorted string based on ASCII order.**

```python
    #Define all element values as strings.
    def _trim_dict(my_dict) :
    # 假设待删除的字典为d
    for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict


    #Remove elements with empty values.
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    # my_dict['timestamp'] = 1749545309665
	#Convert all elements to strings.
    _trim_dict(my_dict)
    #Generate a string sorted by ASCII values and remove special characters.
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    #{"orderId":"2194215","recvWindow":"50000","side":"BUY","symbol":"SANDUSDT","timestamp":"1749545309665","type":"LIMIT"}
```

> **Step 2: Use ABI encoding to combine the string from step 1 with the account information (user, signer) and nonce, then generate a hash string from the encoded result.**

```python
   from eth_abi import encode
   from web3 import Web3

   #Use WEB3 ABI to encode the generated string together with user, signer, and nonce.
   encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
   print(encoded.hex())
   #000000000000000000000000000000000000000000000000000000000000008000000000000000000000000063dd5acc6b1aa0f563956c0e534dd30b6dcf7c4e00000000000000000000000021cf8ae13bb72632562c6fff438652ba1a151bb00000000000000000000000000000000000000000000000000006361457bcec8300000000000000000000000000000000000000000000000000000000000000767b226f726465724964223a2232313934323135222c227265637657696e646f77223a223530303030222c2273696465223a22425559222c2273796d626f6c223a2253414e4455534454222c2274696d657374616d70223a2231373439353435333039363635222c2274797065223a224c494d4954227d00000000000000000000
   keccak_hex =Web3.keccak(encoded).hex()
   print(keccak_hex)
   #6ad9569ea1355bf62de1b09b33b267a9404239af6d9227fa59e3633edae19e2a
```

> **Step 3: Sign the hash generated in Step 2 using the privateKey.**

```python
from eth_account import Account
    from eth_abi import encode
    from web3 import Web3, EthereumTesterProvider
    from eth_account.messages import encode_defunct

    signable_msg = encode_defunct(hexstr=keccak_hex)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    signature =  '0x'+signed_message.signature.hex()
    print(signature)
    #0x4f5e36e91f0d4cf5b29b6559ebc2c808d3c808ebb13b2bcaaa478b98fb4195642c7473f0d1aa101359aaf278126af1a53bcb482fb05003bfb6bdc03de03c63151b
```

> **Step 4: Combine all parameters and the signature from Step 3 into the request body.**

```python
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()

    url ='https://fapi.asterdex.com/fapi/v3/order'

    res = requests.get(url, params=my_dict)
    print(url)
    #curl  -X GET 'https://fapi.asterdex.com/fapi/v3/order?symbol=SANDUSDT&side=BUY&type=LIMIT&orderId=2194215&recvWindow=50000&timestamp=1749545309665&nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0&signature=0x4f5e36e91f0d4cf5b29b6559ebc2c808d3c808ebb13b2bcaaa478b98fb4195642c7473f0d1aa101359aaf278126af1a53bcb482fb05003bfb6bdc03de03c63151b'
```

## python script

```python
#Python 3.9.6
#Python 3.9.6
#eth-account~=0.13.7
#eth-abi~=5.2.0
#web3~=7.11.0
#requests~=2.32.3

import json
import math
import time
import requests

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

user = '0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e'
signer='0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0'
priKey = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
host = 'https://fapi.asterdex.com'
placeOrder = {'url': '/fapi/v3/order', 'method': 'POST',
              'params':{'symbol': 'SANDUSDT', 'positionSide': 'BOTH', 'type': 'LIMIT', 'side': 'BUY',
	         'timeInForce': 'GTC', 'quantity': "30", 'price': 0.325,'reduceOnly': True}}
getOrder = {'url':'/fapi/v3/order','method':'GET','params':{'symbol':'SANDUSDT','side':"BUY","type":'LIMIT','orderId':2194215}}

def call(api):
    nonce = math.trunc(time.time() * 1000000)
    my_dict = api['params']
    send(api['url'],api['method'],sign(my_dict,nonce))

def sign(my_dict,nonce):
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    msg = trim_param(my_dict,nonce)
    signable_msg = encode_defunct(hexstr=msg)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()

    print(my_dict['signature'])
    return  my_dict

def trim_param(my_dict,nonce) -> str:
    _trim_dict(my_dict)
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
    print(encoded.hex())
    keccak_hex =Web3.keccak(encoded).hex()
    print(keccak_hex)
    return keccak_hex

def _trim_dict(my_dict) :
    for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

def send(url, method, my_dict):
    url = host + url
    print(url)
    print(my_dict)
    if method == 'POST':
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }
        res = requests.post(url, data=my_dict, headers=headers)
        print(res.text)
    if method == 'GET':
        res = requests.get(url, params=my_dict)
        print(res.text)
    if method == 'DELETE':
        res = requests.delete(url, data=my_dict)
        print(res.text)

if __name__ == '__main__':
    call(placeOrder)
    # call(getOrder)
```

## Public Endpoints Info

### Terminology

* `base asset` refers to the asset that is the `quantity` of a symbol.
* `quote asset` refers to the asset that is the `price` of a symbol.

### ENUM definitions

**Symbol type:**

* FUTURE

**Contract type (contractType):**

* PERPETUAL

**Contract status(contractStatus，status):**

* PENDING_TRADING
* TRADING
* PRE_SETTLE
* SETTLING
* CLOSE

**Order status (status):**

* NEW
* PARTIALLY_FILLED
* FILLED
* CANCELED
* REJECTED
* EXPIRED

**Order types (orderTypes, type):**

* LIMIT
* MARKET
* STOP
* STOP_MARKET
* TAKE_PROFIT
* TAKE_PROFIT_MARKET
* TRAILING_STOP_MARKET

**Order side (side):**

* BUY
* SELL

**Position side (positionSide):**

* BOTH
* LONG
* SHORT

**Time in force (timeInForce):**

* GTC - Good Till Cancel
* IOC - Immediate or Cancel
* FOK - Fill or Kill
* GTX - Good Till Crossing	(Post Only)

**Working Type (workingType)**

* MARK_PRICE
* CONTRACT_PRICE

**Response Type (newOrderRespType)**

* ACK
* RESULT

**Kline/Candlestick chart intervals:**

m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Rate limiters (rateLimitType)**

> REQUEST_WEIGHT

```javascript
{
  	"rateLimitType": "REQUEST_WEIGHT",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 2400
  }
```

> ORDERS

```javascript
{
  	"rateLimitType": "ORDERS",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 1200
   }
```

* REQUEST_WEIGHT
* ORDERS

**Rate limit intervals (interval)**

* MINUTE

## Filters

Filters define trading rules on a symbol or an exchange.

### Symbol filters

#### PRICE_FILTER

> **/exchangeInfo format:**

```javascript
{
    "filterType": "PRICE_FILTER",
    "minPrice": "0.00000100",
    "maxPrice": "100000.00000000",
    "tickSize": "0.00000100"
  }
```

The `PRICE_FILTER` defines the `price` rules for a symbol. There are 3 parts:

* `minPrice` defines the minimum `price`/`stopPrice` allowed; disabled on `minPrice` == 0.
* `maxPrice` defines the maximum `price`/`stopPrice` allowed; disabled on `maxPrice` == 0.
* `tickSize` defines the intervals that a `price`/`stopPrice` can be increased/decreased by; disabled on `tickSize` == 0.

Any of the above variables can be set to 0, which disables that rule in the `price filter`. In order to pass the `price filter`, the following must be true for `price`/`stopPrice` of the enabled rules:

* `price` >= `minPrice`
* `price` <= `maxPrice`
* (`price`-`minPrice`) % `tickSize` == 0

#### LOT_SIZE

> **/exchangeInfo format:**

```javascript
{
    "filterType": "LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

The `LOT_SIZE` filter defines the `quantity` (aka "lots" in auction terms) rules for a symbol. There are 3 parts:

* `minQty` defines the minimum `quantity` allowed.
* `maxQty` defines the maximum `quantity` allowed.
* `stepSize` defines the intervals that a `quantity` can be increased/decreased by.

In order to pass the `lot size`, the following must be true for `quantity`:

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0

#### MARKET_LOT_SIZE

> **/exchangeInfo format:**

```javascript
{
    "filterType": "MARKET_LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

The `MARKET_LOT_SIZE` filter defines the `quantity` (aka "lots" in auction terms) rules for `MARKET` orders on a symbol. There are 3 parts:

* `minQty` defines the minimum `quantity` allowed.
* `maxQty` defines the maximum `quantity` allowed.
* `stepSize` defines the intervals that a `quantity` can be increased/decreased by.

In order to pass the `market lot size`, the following must be true for `quantity`:

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0

#### MAX_NUM_ORDERS

> **/exchangeInfo format:**

```javascript
{
    "filterType": "MAX_NUM_ORDERS",
    "limit": 200
  }
```

The `MAX_NUM_ORDERS` filter defines the maximum number of orders an account is allowed to have open on a symbol.

Note that both "algo" orders and normal orders are counted for this filter.

#### MAX_NUM_ALGO_ORDERS

> **/exchangeInfo format:**

```javascript
{
    "filterType": "MAX_NUM_ALGO_ORDERS",
    "limit": 100
  }
```

The `MAX_NUM_ALGO_ORDERS ` filter defines the maximum number of all kinds of algo orders an account is allowed to have open on a symbol.

The algo orders include `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`, and `TRAILING_STOP_MARKET` orders.

#### PERCENT_PRICE

> **/exchangeInfo format:**

```javascript
{
    "filterType": "PERCENT_PRICE",
    "multiplierUp": "1.1500",
    "multiplierDown": "0.8500",
    "multiplierDecimal": 4
  }
```

The `PERCENT_PRICE` filter defines valid range for a price based on the mark price.

In order to pass the `percent price`, the following must be true for `price`:

* BUY: `price` <= `markPrice` * `multiplierUp`
* SELL: `price` >= `markPrice` * `multiplierDown`

#### MIN_NOTIONAL

> **/exchangeInfo format:**

```javascript
{
    "filterType": "MIN_NOTIONAL",
    "notional": "1"
  }
```

The `MIN_NOTIONAL` filter defines the minimum notional value allowed for an order on a symbol.
An order's notional value is the `price` * `quantity`.
Since `MARKET` orders have no price, the mark price is used.

---

# Market Data Endpoints

## Test Connectivity

> **Response:**

```javascript
{}
```

``GET /fapi/v1/ping``

Test connectivity to the Rest API.

**Weight:**
1

**Parameters:**
NONE

## Check Server Time

> **Response:**

```javascript
{
  "serverTime": 1499827319559
}
```

``GET /fapi/v1/time``

Test connectivity to the Rest API and get the current server time.

**Weight:**
1

**Parameters:**
NONE

## Exchange Information

> **Response:**

```javascript
{
	"exchangeFilters": [],
 	"rateLimits": [
 		{
 			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 2400,
   			"rateLimitType": "REQUEST_WEIGHT" 
   		},
  		{
  			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 1200,
   			"rateLimitType": "ORDERS"
   		}
   	],
 	"serverTime": 1565613908500,    // Ignore please. If you want to check current server time, please check via "GET /fapi/v1/time"
 	"assets": [ // assets information
 		{
 			"asset": "BUSD",
   			"marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
   			"autoAssetExchange": 0 // auto-exchange threshold in Multi-Assets margin mode
   		},
 		{
 			"asset": "USDT",
   			"marginAvailable": true,
   			"autoAssetExchange": 0
   		},
 		{
 			"asset": "BTC",
   			"marginAvailable": false,
   			"autoAssetExchange": null
   		}
   	],
 	"symbols": [
 		{
 			"symbol": "DOGEUSDT",
 			"pair": "DOGEUSDT",
 			"contractType": "PERPETUAL",
 			"deliveryDate": 4133404800000,
 			"onboardDate": 1598252400000,
 			"status": "TRADING",
 			"maintMarginPercent": "2.5000",   // ignore
 			"requiredMarginPercent": "5.0000",  // ignore
 			"baseAsset": "BLZ", 
 			"quoteAsset": "USDT",
 			"marginAsset": "USDT",
 			"pricePrecision": 5,	// please do not use it as tickSize
 			"quantityPrecision": 0, // please do not use it as stepSize
 			"baseAssetPrecision": 8,
 			"quotePrecision": 8, 
 			"underlyingType": "COIN",
 			"underlyingSubType": ["STORAGE"],
 			"settlePlan": 0,
 			"triggerProtect": "0.15", // threshold for algo order with "priceProtect"
 			"filters": [
 				{
 					"filterType": "PRICE_FILTER",
     				"maxPrice": "300",
     				"minPrice": "0.0001", 
     				"tickSize": "0.0001"
     			},
    			{
    				"filterType": "LOT_SIZE", 
     				"maxQty": "10000000",
     				"minQty": "1",
     				"stepSize": "1"
     			},
    			{
    				"filterType": "MARKET_LOT_SIZE",
     				"maxQty": "590119",
     				"minQty": "1",
     				"stepSize": "1"
     			},
     			{
    				"filterType": "MAX_NUM_ORDERS",
    				"limit": 200
  				},
  				{
    				"filterType": "MAX_NUM_ALGO_ORDERS",
    				"limit": 100
  				},
  				{
  					"filterType": "MIN_NOTIONAL",
  					"notional": "1", 
  				},
  				{
    				"filterType": "PERCENT_PRICE",
    				"multiplierUp": "1.1500",
    				"multiplierDown": "0.8500",
    				"multiplierDecimal": 4
    			}
   			],
 			"OrderType": [
   				"LIMIT",
   				"MARKET",
   				"STOP",
   				"STOP_MARKET",
   				"TAKE_PROFIT",
   				"TAKE_PROFIT_MARKET",
   				"TRAILING_STOP_MARKET" 
   			],
   			"timeInForce": [
   				"GTC", 
   				"IOC", 
   				"FOK", 
   				"GTX" 
 			],
 			"liquidationFee": "0.010000",	// liquidation fee rate
   			"marketTakeBound": "0.30",	// the max price difference rate( from mark price) a market order can make
 		}
   	],
	"timezone": "UTC" 
}
```

``GET /fapi/v1/exchangeInfo``

Current exchange trading rules and symbol information

**Weight:**
1

**Parameters:**
NONE

## Order Book

> **Response:**

```javascript
{
  "lastUpdateId": 1027024,
  "E": 1589436922972,   // Message output time
  "T": 1589436922959,   // Transaction time
  "bids": [
    [
      "4.00000000",     // PRICE
      "431.00000000"    // QTY
    ]
  ],
  "asks": [
    [
      "4.00000200",
      "12.00000000"
    ]
  ]
}
```

``GET /fapi/v1/depth``

**Weight:**

Adjusted based on the limit:

| Limit         | Weight |
| ------------- | ------ |
| 5, 10, 20, 50 | 2      |
| 100           | 5      |
| 500           | 10     |
| 1000          | 20     |

**Parameters:**

| Name   | Type   | Mandatory | Description                                               |
| ------ | ------ | --------- | --------------------------------------------------------- |
| symbol | STRING | YES       |                                                           |
| limit  | INT    | NO        | Default 500; Valid limits:[5, 10, 20, 50, 100, 500, 1000] |

## Recent Trades List

> **Response:**

```javascript
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "48.00",
    "time": 1499865549590,
    "isBuyerMaker": true,
  }
]
```

``GET /fapi/v1/trades``

Get recent market trades

**Weight:**
1

**Parameters:**

| Name   | Type   | Mandatory | Description            |
| ------ | ------ | --------- | ---------------------- |
| symbol | STRING | YES       |                        |
| limit  | INT    | NO        | Default 500; max 1000. |

* Market trades means trades filled in the order book. Only market trades will be returned, which means the insurance fund trades and ADL trades won't be returned.

## Old Trades Lookup (MARKET_DATA)

> **Response:**

```javascript
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "8000.00",
    "time": 1499865549590,
    "isBuyerMaker": true,
  }
]
```

``GET /fapi/v1/historicalTrades``

Get older market historical trades.

**Weight:**
20

**Parameters:**

| Name   | Type   | Mandatory | Description                                             |
| ------ | ------ | --------- | ------------------------------------------------------- |
| symbol | STRING | YES       |                                                         |
| limit  | INT    | NO        | Default 500; max 1000.                                  |
| fromId | LONG   | NO        | TradeId to fetch from. Default gets most recent trades. |

* Market trades means trades filled in the order book. Only market trades will be returned, which means the insurance fund trades and ADL trades won't be returned.

## Compressed/Aggregate Trades List

> **Response:**

```javascript
[
  {
    "a": 26129,         // Aggregate tradeId
    "p": "0.01633102",  // Price
    "q": "4.70443515",  // Quantity
    "f": 27781,         // First tradeId
    "l": 27781,         // Last tradeId
    "T": 1498793709153, // Timestamp
    "m": true,          // Was the buyer the maker?
  }
]
```

``GET /fapi/v1/aggTrades``

Get compressed, aggregate market trades. Market trades that fill at the time, from the same order, with the same price will have the quantity aggregated.

**Weight:**
20

**Parameters:**

| Name      | Type   | Mandatory | Description                                              |
| --------- | ------ | --------- | -------------------------------------------------------- |
| symbol    | STRING | YES       |                                                          |
| fromId    | LONG   | NO        | ID to get aggregate trades from INCLUSIVE.               |
| startTime | LONG   | NO        | Timestamp in ms to get aggregate trades from INCLUSIVE.  |
| endTime   | LONG   | NO        | Timestamp in ms to get aggregate trades until INCLUSIVE. |
| limit     | INT    | NO        | Default 500; max 1000.                                   |

* If both startTime and endTime are sent, time between startTime and endTime must be less than 1 hour.
* If fromId, startTime, and endTime are not sent, the most recent aggregate trades will be returned.
* Only market trades will be aggregated and returned, which means the insurance fund trades and ADL trades won't be aggregated.

## Kline/Candlestick Data

> **Response:**

```javascript
[
  [
    1499040000000,      // Open time
    "0.01634790",       // Open
    "0.80000000",       // High
    "0.01575800",       // Low
    "0.01577100",       // Close
    "148976.11427815",  // Volume
    1499644799999,      // Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "17928899.62484339" // Ignore.
  ]
]
```

``GET /fapi/v1/klines``

Kline/candlestick bars for a symbol.
Klines are uniquely identified by their open time.

**Weight:** based on parameter `LIMIT`

| LIMIT       | weight |
| ----------- | ------ |
| [1,100)     | 1      |
| [100, 500)  | 2      |
| [500, 1000] | 5      |

> 1000 | 10

**Parameters:**

| Name      | Type   | Mandatory | Description            |
| --------- | ------ | --------- | ---------------------- |
| symbol    | STRING | YES       |                        |
| interval  | ENUM   | YES       |                        |
| startTime | LONG   | NO        |                        |
| endTime   | LONG   | NO        |                        |
| limit     | INT    | NO        | Default 500; max 1500. |

* If startTime and endTime are not sent, the most recent klines are returned.

## Index Price Kline/Candlestick Data

> **Response:**

```javascript
[
  [
    1591256400000,      	// Open time
    "9653.69440000",    	// Open
    "9653.69640000",     	// High
    "9651.38600000",     	// Low
    "9651.55200000",     	// Close (or latest price)
    "0	", 					// Ignore
    1591256459999,      	// Close time
    "0",    				// Ignore
    60,                		// Number of bisic data
    "0",    				// Ignore
    "0",      				// Ignore
    "0" 					// Ignore
  ]
]
```

``GET /fapi/v1/indexPriceKlines``

Kline/candlestick bars for the index price of a pair.

Klines are uniquely identified by their open time.

**Weight:** based on parameter `LIMIT`

| LIMIT       | weight |
| ----------- | ------ |
| [1,100)     | 1      |
| [100, 500)  | 2      |
| [500, 1000] | 5      |

> 1000 | 10

**Parameters:**

| Name      | Type   | Mandatory | Description            |
| --------- | ------ | --------- | ---------------------- |
| pair      | STRING | YES       |                        |
| interval  | ENUM   | YES       |                        |
| startTime | LONG   | NO        |                        |
| endTime   | LONG   | NO        |                        |
| limit     | INT    | NO        | Default 500; max 1500. |

* If startTime and endTime are not sent, the most recent klines are returned.

## Mark Price Kline/Candlestick Data

> **Response:**

```javascript
[
  [
    1591256460000,     		// Open time
    "9653.29201333",    	// Open
    "9654.56401333",     	// High
    "9653.07367333",     	// Low
    "9653.07367333",     	// Close (or latest price)
    "0	", 					// Ignore
    1591256519999,      	// Close time
    "0",    				// Ignore
    60,                	 	// Number of bisic data
    "0",    				// Ignore
    "0",      			 	// Ignore
    "0" 					// Ignore
  ]
]
```

``GET /fapi/v1/markPriceKlines``

Kline/candlestick bars for the mark price of a symbol.

Klines are uniquely identified by their open time.

**Weight:** based on parameter `LIMIT`

| LIMIT       | weight |
| ----------- | ------ |
| [1,100)     | 1      |
| [100, 500)  | 2      |
| [500, 1000] | 5      |

> 1000 | 10

**Parameters:**

| Name      | Type   | Mandatory | Description            |
| --------- | ------ | --------- | ---------------------- |
| symbol    | STRING | YES       |                        |
| interval  | ENUM   | YES       |                        |
| startTime | LONG   | NO        |                        |
| endTime   | LONG   | NO        |                        |
| limit     | INT    | NO        | Default 500; max 1500. |

* If startTime and endTime are not sent, the most recent klines are returned.

## Mark Price

> **Response:**

```javascript
{
	"symbol": "BTCUSDT",
	"markPrice": "11793.63104562",	// mark price
	"indexPrice": "11781.80495970",	// index price
	"estimatedSettlePrice": "11781.16138815", // Estimated Settle Price, only useful in the last hour before the settlement starts.
	"lastFundingRate": "0.00038246",  // This is the lasted funding rate
	"nextFundingTime": 1597392000000,
	"interestRate": "0.00010000",
	"time": 1597370495002
}
```

> **OR (when symbol not sent)**

```javascript
[
	{
	    "symbol": "BTCUSDT",
	    "markPrice": "11793.63104562",	// mark price
	    "indexPrice": "11781.80495970",	// index price
	    "estimatedSettlePrice": "11781.16138815", // Estimated Settle Price, only useful in the last hour before the settlement starts.
	    "lastFundingRate": "0.00038246",  // This is the lasted funding rate
	    "nextFundingTime": 1597392000000,
	    "interestRate": "0.00010000",
	    "time": 1597370495002
	}
]
```

``GET /fapi/v1/premiumIndex``

Mark Price and Funding Rate

**Weight:**
1

**Parameters:**

| Name   | Type   | Mandatory | Description |
| ------ | ------ | --------- | ----------- |
| symbol | STRING | NO        |             |

## Get Funding Rate History

> **Response:**

```javascript
[
	{
    	"symbol": "BTCUSDT",
    	"fundingRate": "-0.03750000",
    	"fundingTime": 1570608000000,
	},
	{
   		"symbol": "BTCUSDT",
    	"fundingRate": "0.00010000",
    	"fundingTime": 1570636800000,
	}
]
```

``GET /fapi/v1/fundingRate``

**Weight:**
1

**Parameters:**

| Name      | Type   | Mandatory | Description                                           |
| --------- | ------ | --------- | ----------------------------------------------------- |
| symbol    | STRING | NO        |                                                       |
| startTime | LONG   | NO        | Timestamp in ms to get funding rate from INCLUSIVE.   |
| endTime   | LONG   | NO        | Timestamp in ms to get funding rate  until INCLUSIVE. |
| limit     | INT    | NO        | Default 100; max 1000                                 |

* If `startTime` and `endTime` are not sent, the most recent `limit` datas are returned.
* If the number of data between `startTime` and `endTime` is larger than `limit`, return as `startTime` + `limit`.
* In ascending order.

## 24hr Ticker Price Change Statistics

> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "priceChange": "-94.99999800",
  "priceChangePercent": "-95.960",
  "weightedAvgPrice": "0.29628482",
  "prevClosePrice": "0.10002000",
  "lastPrice": "4.00000200",
  "lastQty": "200.00000000",
  "openPrice": "99.00000000",
  "highPrice": "100.00000000",
  "lowPrice": "0.10000000",
  "volume": "8913.30000000",
  "quoteVolume": "15.30000000",
  "openTime": 1499783499040,
  "closeTime": 1499869899040,
  "firstId": 28385,   // First tradeId
  "lastId": 28460,    // Last tradeId
  "count": 76         // Trade count
}
```

> OR

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"priceChange": "-94.99999800",
  		"priceChangePercent": "-95.960",
  		"weightedAvgPrice": "0.29628482",
  		"prevClosePrice": "0.10002000",
  		"lastPrice": "4.00000200",
  		"lastQty": "200.00000000",
  		"openPrice": "99.00000000",
  		"highPrice": "100.00000000",
  		"lowPrice": "0.10000000",
  		"volume": "8913.30000000",
  		"quoteVolume": "15.30000000",
  		"openTime": 1499783499040,
  		"closeTime": 1499869899040,
  		"firstId": 28385,   // First tradeId
  		"lastId": 28460,    // Last tradeId
  		"count": 76         // Trade count
	}
]
```

``GET /fapi/v1/ticker/24hr``

24 hour rolling window price change statistics.
**Careful** when accessing this with no symbol.

**Weight:**
1 for a single symbol;
**40** when the symbol parameter is omitted

**Parameters:**

| Name   | Type   | Mandatory | Description |
| ------ | ------ | --------- | ----------- |
| symbol | STRING | NO        |             |

* If the symbol is not sent, tickers for all symbols will be returned in an array.

## Symbol Price Ticker

> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "price": "6000.01",
  "time": 1589437530011   // Transaction time
}
```

> OR

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"price": "6000.01",
  		"time": 1589437530011
	}
]
```

``GET /fapi/v1/ticker/price``

Latest price for a symbol or symbols.

**Weight:**
1 for a single symbol;
**2** when the symbol parameter is omitted

**Parameters:**

| Name   | Type   | Mandatory | Description |
| ------ | ------ | --------- | ----------- |
| symbol | STRING | NO        |             |

* If the symbol is not sent, prices for all symbols will be returned in an array.

## Symbol Order Book Ticker

> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "bidPrice": "4.00000000",
  "bidQty": "431.00000000",
  "askPrice": "4.00000200",
  "askQty": "9.00000000",
  "time": 1589437530011   // Transaction time
}
```

> OR

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"bidPrice": "4.00000000",
  		"bidQty": "431.00000000",
  		"askPrice": "4.00000200",
  		"askQty": "9.00000000",
  		"time": 1589437530011
	}
]
```

``GET /fapi/v1/ticker/bookTicker``

Best price/qty on the order book for a symbol or symbols.

**Weight:**
1 for a single symbol;
**2** when the symbol parameter is omitted

**Parameters:**

| Name   | Type   | Mandatory | Description |
| ------ | ------ | --------- | ----------- |
| symbol | STRING | NO        |             |

* If the symbol is not sent, bookTickers for all symbols will be returned in an array.

# Websocket Market Streams

* The baseurl for websocket is **wss://fstream.asterdex.com**
* Streams can be access either in a single raw stream or a combined stream
* Raw streams are accessed at **/ws/\<streamName\>**
* Combined streams are accessed at **/stream?streams=\<streamName1\>/\<streamName2\>/\<streamName3\>**
* Combined stream events are wrapped as follows: **{"stream":"\<streamName\>","data":\<rawPayload\>}**
* All symbols for streams are **lowercase**
* A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
* The websocket server will send a `ping frame` every 5 minutes. If the websocket server does not receive a `pong frame` back from the connection within a 15 minute period, the connection will be disconnected. Unsolicited `pong frames` are allowed.
* WebSocket connections have a limit of 10 incoming messages per second.
* A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.
* A single connection can listen to a maximum of **200** streams.
* Considering the possible data latency from RESTful endpoints during an extremely volatile market, it is highly recommended to get the order status, position, etc from the Websocket user data stream.

## Live Subscribing/Unsubscribing to streams

* The following data can be sent through the websocket instance in order to subscribe/unsubscribe from streams. Examples can be seen below.
* The `id` used in the JSON payloads is an unsigned INT used as an identifier to uniquely identify the messages going back and forth.

### Subscribe to a stream

> **Response**

```javascript
{
  "result": null,
  "id": 1
}
```

* **Request**
  
  {
  "method": "SUBSCRIBE",
  "params":
  [
  "btcusdt@aggTrade",
  "btcusdt@depth"
  ],
  "id": 1
  }

### Unsubscribe to a stream

> **Response**

```javascript
{
  "result": null,
  "id": 312
}
```

* **Request**
  
  {
  "method": "UNSUBSCRIBE",
  "params":
  [
  "btcusdt@depth"
  ],
  "id": 312
  }

### Listing Subscriptions

> **Response**

```javascript
{
  "result": [
    "btcusdt@aggTrade"
  ],
  "id": 3
}
```

* **Request**
  
  {
  "method": "LIST_SUBSCRIPTIONS",
  "id": 3
  }

### Setting Properties

Currently, the only property can be set is to set whether `combined` stream payloads are enabled are not.
The combined property is set to `false` when connecting using `/ws/` ("raw streams") and `true` when connecting using `/stream/`.

> **Response**

```javascript
{
  "result": null,
  "id": 5
}
```

* **Request**
  
  {
  "method": "SET_PROPERTY",
  "params":
  [
  "combined",
  true
  ],
  "id": 5
  }

### Retrieving Properties

> **Response**

```javascript
{
  "result": true, // Indicates that combined is set to true.
  "id": 2
}
```

* **Request**
  
  {
  "method": "GET_PROPERTY",
  "params":
  [
  "combined"
  ],
  "id": 2
  }

### Error Messages

| Error Message                                                                                                                                                                  | Description                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| {"code": 0, "msg": "Unknown property"}                                                                                                                                         | Parameter used in the`SET_PROPERTY` or `GET_PROPERTY` was invalid                                   |
| {"code": 1, "msg": "Invalid value type: expected Boolean"}                                                                                                                     | Value should only be`true` or `false`                                                               |
| {"code": 2, "msg": "Invalid request: property name must be a string"}                                                                                                          | Property name provided was invalid                                                                  |
| {"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"}                                                                                                  | Parameter`id` had to be provided or the value provided in the `id` parameter is an unsupported type |
| {"code": 2, "msg": "Invalid request: unknown variant %s, expected one of`SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28"} | Possible typo in the provided method or provided method was neither of the expected values          |
| {"code": 2, "msg": "Invalid request: too many parameters"}                                                                                                                     | Unnecessary parameters provided in the data                                                         |
| {"code": 2, "msg": "Invalid request: property name must be a string"}                                                                                                          | Property name was not provided                                                                      |
| {"code": 2, "msg": "Invalid request: missing field`method` at line 1 column 73"}                                                                                               | `method` was not provided in the data                                                               |
| {"code":3,"msg":"Invalid JSON: expected value at line %s column %s"}                                                                                                           | JSON data sent has incorrect syntax.                                                                |

## Aggregate Trade Streams

> **Payload:**

```javascript
{
  "e": "aggTrade",  // Event type
  "E": 123456789,   // Event time
  "s": "BTCUSDT",    // Symbol
  "a": 5933014,		// Aggregate trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "f": 100,         // First trade ID
  "l": 105,         // Last trade ID
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
}
```

The Aggregate Trade Streams push market trade information that is aggregated for a single taker order every 100 milliseconds.

**Stream Name:**
``<symbol>@aggTrade``

**Update Speed:** 100ms

* Only market trades will be aggregated, which means the insurance fund trades and ADL trades won't be aggregated.

## Mark Price Stream

> **Payload:**

```javascript
{
    "e": "markPriceUpdate",  	// Event type
    "E": 1562305380000,      	// Event time
    "s": "BTCUSDT",          	// Symbol
    "p": "11794.15000000",   	// Mark price
    "i": "11784.62659091",		// Index price
    "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
    "r": "0.00038167",       	// Funding rate
    "T": 1562306400000       	// Next funding time
  }
```

Mark price and funding rate for a single symbol pushed every 3 seconds or every second.

**Stream Name:**
``<symbol>@markPrice`` or ``<symbol>@markPrice@1s``

**Update Speed:** 3000ms or 1000ms

## Mark Price Stream for All market

> **Payload:**

```javascript
[ 
  {
    "e": "markPriceUpdate",  	// Event type
    "E": 1562305380000,      	// Event time
    "s": "BTCUSDT",          	// Symbol
    "p": "11185.87786614",   	// Mark price
    "i": "11784.62659091"		// Index price
    "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
    "r": "0.00030000",       	// Funding rate
    "T": 1562306400000       	// Next funding time
  }
]
```

Mark price and funding rate for all symbols pushed every 3 seconds or every second.

**Stream Name:**
``!markPrice@arr`` or ``!markPrice@arr@1s``

**Update Speed:** 3000ms or 1000ms

## Kline/Candlestick Streams

> **Payload:**

```javascript
{
  "e": "kline",     // Event type
  "E": 123456789,   // Event time
  "s": "BTCUSDT",    // Symbol
  "k": {
    "t": 123400000, // Kline start time
    "T": 123460000, // Kline close time
    "s": "BTCUSDT",  // Symbol
    "i": "1m",      // Interval
    "f": 100,       // First trade ID
    "L": 200,       // Last trade ID
    "o": "0.0010",  // Open price
    "c": "0.0020",  // Close price
    "h": "0.0025",  // High price
    "l": "0.0015",  // Low price
    "v": "1000",    // Base asset volume
    "n": 100,       // Number of trades
    "x": false,     // Is this kline closed?
    "q": "1.0000",  // Quote asset volume
    "V": "500",     // Taker buy base asset volume
    "Q": "0.500",   // Taker buy quote asset volume
    "B": "123456"   // Ignore
  }
}
```

The Kline/Candlestick Stream push updates to the current klines/candlestick every 250 milliseconds (if existing).

**Kline/Candlestick chart intervals:**

m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Stream Name:**
``<symbol>@kline_<interval>``

**Update Speed:** 250ms

## Individual Symbol Mini Ticker Stream

> **Payload:**

```javascript
{
    "e": "24hrMiniTicker",  // Event type
    "E": 123456789,         // Event time
    "s": "BTCUSDT",         // Symbol
    "c": "0.0025",          // Close price
    "o": "0.0010",          // Open price
    "h": "0.0025",          // High price
    "l": "0.0010",          // Low price
    "v": "10000",           // Total traded base asset volume
    "q": "18"               // Total traded quote asset volume
  }
```

24hr rolling window mini-ticker statistics for a single symbol. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before.

**Stream Name:**
``<symbol>@miniTicker``

**Update Speed:** 500ms

## All Market Mini Tickers Stream

> **Payload:**

```javascript
[  
  {
    "e": "24hrMiniTicker",  // Event type
    "E": 123456789,         // Event time
    "s": "BTCUSDT",         // Symbol
    "c": "0.0025",          // Close price
    "o": "0.0010",          // Open price
    "h": "0.0025",          // High price
    "l": "0.0010",          // Low price
    "v": "10000",           // Total traded base asset volume
    "q": "18"               // Total traded quote asset volume
  }
]
```

24hr rolling window mini-ticker statistics for all symbols. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before. Note that only tickers that have changed will be present in the array.

**Stream Name:**
``!miniTicker@arr``

**Update Speed:** 1000ms

## Individual Symbol Ticker Streams

> **Payload:**

```javascript
{
  "e": "24hrTicker",  // Event type
  "E": 123456789,     // Event time
  "s": "BTCUSDT",     // Symbol
  "p": "0.0015",      // Price change
  "P": "250.00",      // Price change percent
  "w": "0.0018",      // Weighted average price
  "c": "0.0025",      // Last price
  "Q": "10",          // Last quantity
  "o": "0.0010",      // Open price
  "h": "0.0025",      // High price
  "l": "0.0010",      // Low price
  "v": "10000",       // Total traded base asset volume
  "q": "18",          // Total traded quote asset volume
  "O": 0,             // Statistics open time
  "C": 86400000,      // Statistics close time
  "F": 0,             // First trade ID
  "L": 18150,         // Last trade Id
  "n": 18151          // Total number of trades
}
```

24hr rollwing window ticker statistics for a single symbol. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before.

**Stream Name:**
``<symbol>@ticker``

**Update Speed:** 500ms

## All Market Tickers Streams

> **Payload:**

```javascript
[
	{
	  "e": "24hrTicker",  // Event type
	  "E": 123456789,     // Event time
	  "s": "BTCUSDT",     // Symbol
	  "p": "0.0015",      // Price change
	  "P": "250.00",      // Price change percent
	  "w": "0.0018",      // Weighted average price
	  "c": "0.0025",      // Last price
	  "Q": "10",          // Last quantity
	  "o": "0.0010",      // Open price
	  "h": "0.0025",      // High price
	  "l": "0.0010",      // Low price
	  "v": "10000",       // Total traded base asset volume
	  "q": "18",          // Total traded quote asset volume
	  "O": 0,             // Statistics open time
	  "C": 86400000,      // Statistics close time
	  "F": 0,             // First trade ID
	  "L": 18150,         // Last trade Id
	  "n": 18151          // Total number of trades
	}
]
```

24hr rollwing window ticker statistics for all symbols. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before. Note that only tickers that have changed will be present in the array.

**Stream Name:**
``!ticker@arr``

**Update Speed:** 1000ms

## Individual Symbol Book Ticker Streams

> **Payload:**

```javascript
{
  "e":"bookTicker",			// event type
  "u":400900217,     		// order book updateId
  "E": 1568014460893,  		// event time
  "T": 1568014460891,  		// transaction time
  "s":"BNBUSDT",     		// symbol
  "b":"25.35190000", 		// best bid price
  "B":"31.21000000", 		// best bid qty
  "a":"25.36520000", 		// best ask price
  "A":"40.66000000"  		// best ask qty
}
```

Pushes any update to the best bid or ask's price or quantity in real-time for a specified symbol.

**Stream Name:** `<symbol>@bookTicker`

**Update Speed:** Real-time

## All Book Tickers Stream

> **Payload:**

```javascript
{
  // Same as <symbol>@bookTicker payload
}
```

Pushes any update to the best bid or ask's price or quantity in real-time for all symbols.

**Stream Name:** `!bookTicker`

**Update Speed:** Real-time

## Liquidation Order Streams

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // Event Type
	"E":1568014460893,                  // Event Time
	"o":{

		"s":"BTCUSDT",                   // Symbol
		"S":"SELL",                      // Side
		"o":"LIMIT",                     // Order Type
		"f":"IOC",                       // Time in Force
		"q":"0.014",                     // Original Quantity
		"p":"9910",                      // Price
		"ap":"9910",                     // Average Price
		"X":"FILLED",                    // Order Status
		"l":"0.014",                     // Order Last Filled Quantity
		"z":"0.014",                     // Order Filled Accumulated Quantity
		"T":1568014460893,          	 // Order Trade Time

	}

}
```

The Liquidation Order Snapshot Streams push force liquidation order information for specific symbol.

For each symbol，only the latest one liquidation order within 1000ms will be pushed as the snapshot. If no liquidation happens in the interval of 1000ms, no stream will be pushed.

**Stream Name:**  ``<symbol>@forceOrder``

**Update Speed:** 1000ms

## All Market Liquidation Order Streams

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // Event Type
	"E":1568014460893,                  // Event Time
	"o":{

		"s":"BTCUSDT",                   // Symbol
		"S":"SELL",                      // Side
		"o":"LIMIT",                     // Order Type
		"f":"IOC",                       // Time in Force
		"q":"0.014",                     // Original Quantity
		"p":"9910",                      // Price
		"ap":"9910",                     // Average Price
		"X":"FILLED",                    // Order Status
		"l":"0.014",                     // Order Last Filled Quantity
		"z":"0.014",                     // Order Filled Accumulated Quantity
		"T":1568014460893,          	 // Order Trade Time

	}

}
```

The All Liquidation Order Snapshot Streams push force liquidation order information for all symbols in the market.

For each symbol，only the latest one liquidation order within 1000ms will be pushed as the snapshot. If no liquidation happens in the interval of 1000ms, no stream will be pushed.

**Stream Name:** ``!forceOrder@arr``

**Update Speed:** 1000ms

## Partial Book Depth Streams

> **Payload:**

```javascript
{
  "e": "depthUpdate", // Event type
  "E": 1571889248277, // Event time
  "T": 1571889248276, // Transaction time
  "s": "BTCUSDT",
  "U": 390497796,
  "u": 390497878,
  "pu": 390497794,
  "b": [          // Bids to be updated
    [
      "7403.89",  // Price Level to be
      "0.002"     // Quantity
    ],
    [
      "7403.90",
      "3.906"
    ],
    [
      "7404.00",
      "1.428"
    ],
    [
      "7404.85",
      "5.239"
    ],
    [
      "7405.43",
      "2.562"
    ]
  ],
  "a": [          // Asks to be updated
    [
      "7405.96",  // Price level to be
      "3.340"     // Quantity
    ],
    [
      "7406.63",
      "4.525"
    ],
    [
      "7407.08",
      "2.475"
    ],
    [
      "7407.15",
      "4.800"
    ],
    [
      "7407.20",
      "0.175"
    ]
  ]
}
```

Top **<levels\>** bids and asks, Valid **<levels\>** are 5, 10, or 20.

**Stream Names:** `<symbol>@depth<levels>` OR `<symbol>@depth<levels>@500ms` OR `<symbol>@depth<levels>@100ms`.

**Update Speed:** 250ms, 500ms or 100ms

## Diff. Book Depth Streams

> **Payload:**

```javascript
{
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 157,           // First update ID in event
  "u": 160,           // Final update ID in event
  "pu": 149,          // Final update Id in last stream(ie `u` in last stream)
  "b": [              // Bids to be updated
    [
      "0.0024",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "0.0026",       // Price level to be updated
      "100"          // Quantity
    ]
  ]
}
```

Bids and asks, pushed every 250 milliseconds, 500 milliseconds, 100 milliseconds (if existing)

**Stream Name:**
``<symbol>@depth`` OR ``<symbol>@depth@500ms``  OR ``<symbol>@depth@100ms``

**Update Speed:** 250ms, 500ms, 100ms

## How to manage a local order book correctly

1. Open a stream to **wss://fstream.asterdex.com/stream?streams=btcusdt@depth**.
2. Buffer the events you receive from the stream. For same price, latest received update covers the previous one.
3. Get a depth snapshot from **https://fapi.asterdex.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000** .
4. Drop any event where `u` is < `lastUpdateId` in the snapshot.
5. The first processed event should have `U` <= `lastUpdateId` **AND** `u` >= `lastUpdateId`
6. While listening to the stream, each new event's `pu` should be equal to the previous event's `u`, otherwise initialize the process from step 3.
7. The data in each event is the **absolute** quantity for a price level.
8. If the quantity is 0, **remove** the price level.
9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.

# Account/Trades Endpoints

<aside class="warning">
Considering the possible data latency from RESTful endpoints during an extremely volatile market, it is highly recommended to get the order status, position, etc from the Websocket user data stream.
</aside>

## Change Position Mode(TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``POST /fapi/v1/positionSide/dual (HMAC SHA256)``

Change user's position mode (Hedge Mode or One-way Mode ) on ***EVERY symbol***

**Weight:**
1

**Parameters:**

| Name             | Type   | Mandatory | Description                               |
| ---------------- | ------ | --------- | ----------------------------------------- |
| dualSidePosition | STRING | YES       | "true": Hedge Mode; "false": One-way Mode |
| recvWindow       | LONG   | NO        |                                           |
| timestamp        | LONG   | YES       |                                           |

## Get Current Position Mode(USER_DATA)

> **Response:**

```javascript
{
	"dualSidePosition": true // "true": Hedge Mode; "false": One-way Mode
}
```

``GET /fapi/v1/positionSide/dual (HMAC SHA256)``

Get user's position mode (Hedge Mode or One-way Mode ) on ***EVERY symbol***

**Weight:**
30

**Parameters:**

| Name       | Type | Mandatory | Description |
| ---------- | ---- | --------- | ----------- |
| recvWindow | LONG | NO        |             |
| timestamp  | LONG | YES       |             |

## Change Multi-Assets Mode (TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``POST /fapi/v1/multiAssetsMargin (HMAC SHA256)``

Change user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on ***Every symbol***

**Weight:**
1

**Parameters:**

| Name              | Type   | Mandatory | Description                                           |
| ----------------- | ------ | --------- | ----------------------------------------------------- |
| multiAssetsMargin | STRING | YES       | "true": Multi-Assets Mode; "false": Single-Asset Mode |
| recvWindow        | LONG   | NO        |                                                       |
| timestamp         | LONG   | YES       |                                                       |

## Get Current Multi-Assets Mode (USER_DATA)

> **Response:**

```javascript
{
	"multiAssetsMargin": true // "true": Multi-Assets Mode; "false": Single-Asset Mode
}
```

``GET /fapi/v1/multiAssetsMargin (HMAC SHA256)``

Get user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on ***Every symbol***

**Weight:**
30

**Parameters:**

| Name       | Type | Mandatory | Description |
| ---------- | ---- | --------- | ----------- |
| recvWindow | LONG | NO        |             |
| timestamp  | LONG | YES       |             |

## New Order  (TRADE)

> **Response:**

```javascript
{
 	"clientOrderId": "testOrder",
 	"cumQty": "0",
 	"cumQuote": "0",
 	"executedQty": "0",
 	"orderId": 22542179,
 	"avgPrice": "0.00000",
 	"origQty": "10",
 	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",		// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"origType": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",	// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",			// callback rate, only return with TRAILING_STOP_MARKET order
 	"updateTime": 1566818724722,
 	"workingType": "CONTRACT_PRICE",
 	"priceProtect": false            // if conditional order trigger is protected
}
```

``POST /fapi/v1/order  (HMAC SHA256)``

Send in a new order.

**Weight:**
1

**Parameters:**

| Name             | Type    | Mandatory | Description                                                                                                                            |
| ---------------- | ------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| symbol           | STRING  | YES       |                                                                                                                                        |
| side             | ENUM    | YES       |                                                                                                                                        |
| positionSide     | ENUM    | NO        | Default`BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent in Hedge Mode.                                      |
| type             | ENUM    | YES       |                                                                                                                                        |
| timeInForce      | ENUM    | NO        |                                                                                                                                        |
| quantity         | DECIMAL | NO        | Cannot be sent with`closePosition`=`true`(Close-All)                                                                                   |
| reduceOnly       | STRING  | NO        | "true" or "false". default "false". Cannot be sent in Hedge Mode; cannot be sent with`closePosition`=`true`                            |
| price            | DECIMAL | NO        |                                                                                                                                        |
| newClientOrderId | STRING  | NO        | A unique id among open orders. Automatically generated if not sent. Can only be string following the rule:`^[\.A-Z\:/a-z0-9_-]{1,36}$` |
| stopPrice        | DECIMAL | NO        | Used with`STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.                                                                |
| closePosition    | STRING  | NO        | `true`, `false`；Close-All，used with `STOP_MARKET` or `TAKE_PROFIT_MARKET`.                                                           |
| activationPrice  | DECIMAL | NO        | Used with`TRAILING_STOP_MARKET` orders, default as the latest price(supporting different `workingType`)                                |
| callbackRate     | DECIMAL | NO        | Used with`TRAILING_STOP_MARKET` orders, min 0.1, max 5 where 1 for 1%                                                                  |
| workingType      | ENUM    | NO        | stopPrice triggered by: "MARK_PRICE", "CONTRACT_PRICE". Default "CONTRACT_PRICE"                                                       |
| priceProtect     | STRING  | NO        | "TRUE" or "FALSE", default "FALSE". Used with`STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.                            |
| newOrderRespType | ENUM    | NO        | "ACK", "RESULT", default "ACK"                                                                                                         |
| recvWindow       | LONG    | NO        |                                                                                                                                        |
| timestamp        | LONG    | YES       |                                                                                                                                        |

Additional mandatory parameters based on `type`:

| Type                             | Additional mandatory parameters    |
| -------------------------------- | ---------------------------------- |
| `LIMIT`                          | `timeInForce`, `quantity`, `price` |
| `MARKET`                         | `quantity`                         |
| `STOP/TAKE_PROFIT`               | `quantity`,  `price`, `stopPrice`  |
| `STOP_MARKET/TAKE_PROFIT_MARKET` | `stopPrice`                        |
| `TRAILING_STOP_MARKET`           | `callbackRate`                     |

* Order with type `STOP`,  parameter `timeInForce` can be sent ( default `GTC`).
* Order with type `TAKE_PROFIT`,  parameter `timeInForce` can be sent ( default `GTC`).
* Condition orders will be triggered when:
  
  * If parameter`priceProtect`is sent as true:
    
    * when price reaches the `stopPrice` ，the difference rate between "MARK_PRICE" and "CONTRACT_PRICE" cannot be larger than the "triggerProtect" of the symbol
    * "triggerProtect" of a symbol can be got from `GET /fapi/v1/exchangeInfo`
  * `STOP`, `STOP_MARKET`:
    
    * BUY: latest price ("MARK_PRICE" or "CONTRACT_PRICE") >= `stopPrice`
    * SELL: latest price ("MARK_PRICE" or "CONTRACT_PRICE") <= `stopPrice`
  * `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`:
    
    * BUY: latest price ("MARK_PRICE" or "CONTRACT_PRICE") <= `stopPrice`
    * SELL: latest price ("MARK_PRICE" or "CONTRACT_PRICE") >= `stopPrice`
  * `TRAILING_STOP_MARKET`:
    
    * BUY: the lowest price after order placed <= `activationPrice`, and the latest price >= the lowest price * (1 + `callbackRate`)
    * SELL: the highest price after order placed >= `activationPrice`, and the latest price <= the highest price * (1 - `callbackRate`)
* For `TRAILING_STOP_MARKET`, if you got such error code.``{"code": -2021, "msg": "Order would immediately trigger."}``means that the parameters you send do not meet the following requirements:
  
  * BUY: `activationPrice` should be smaller than latest price.
  * SELL: `activationPrice` should be larger than latest price.
* If `newOrderRespType ` is sent as `RESULT` :
  
  * `MARKET` order: the final FILLED result of the order will be return directly.
  * `LIMIT` order with special `timeInForce`: the final status result of the order(FILLED or EXPIRED) will be returned directly.
* `STOP_MARKET`, `TAKE_PROFIT_MARKET` with `closePosition`=`true`:
  
  * Follow the same rules for condition orders.
  * If triggered，**close all** current long position( if `SELL`) or current short position( if `BUY`).
  * Cannot be used with `quantity` paremeter
  * Cannot be used with `reduceOnly` parameter
  * In Hedge Mode,cannot be used with `BUY` orders in `LONG` position side. and cannot be used with `SELL` orders in `SHORT` position side

## Place Multiple Orders  (TRADE)

> **Response:**

```javascript
[
	{
	 	"clientOrderId": "testOrder",
	 	"cumQty": "0",
	 	"cumQuote": "0",
	 	"executedQty": "0",
	 	"orderId": 22542179,
	 	"avgPrice": "0.00000",
	 	"origQty": "10",
	 	"price": "0",
	  	"reduceOnly": false,
	  	"side": "BUY",
	  	"positionSide": "SHORT",
	  	"status": "NEW",
	  	"stopPrice": "9300",		// please ignore when order type is TRAILING_STOP_MARKET
	  	"symbol": "BTCUSDT",
	  	"timeInForce": "GTC",
	  	"type": "TRAILING_STOP_MARKET",
	  	"origType": "TRAILING_STOP_MARKET",
	  	"activatePrice": "9020",	// activation price, only return with TRAILING_STOP_MARKET order
	  	"priceRate": "0.3",			// callback rate, only return with TRAILING_STOP_MARKET order
	 	"updateTime": 1566818724722,
	 	"workingType": "CONTRACT_PRICE",
	 	"priceProtect": false            // if conditional order trigger is protected
	},
	{
		"code": -2022, 
		"msg": "ReduceOnly Order is rejected."
	}
]
```

``POST /fapi/v1/batchOrders  (HMAC SHA256)``

**Weight:**
5

**Parameters:**

| Name        | Type       | Mandatory | Description              |
| ----------- | ---------- | --------- | ------------------------ |
| batchOrders | LIST | YES       | order list. Max 5 orders |
| recvWindow  | LONG       | NO        |                          |
| timestamp   | LONG       | YES       |                          |

**Where ``batchOrders`` is the list of order parameters in JSON**

| Name             | Type    | Mandatory | Description                                                                                                                            |
| ---------------- | ------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| symbol           | STRING  | YES       |                                                                                                                                        |
| side             | ENUM    | YES       |                                                                                                                                        |
| positionSide     | ENUM    | NO        | Default`BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent with Hedge Mode.                                    |
| type             | ENUM    | YES       |                                                                                                                                        |
| timeInForce      | ENUM    | NO        |                                                                                                                                        |
| quantity         | DECIMAL | YES       |                                                                                                                                        |
| reduceOnly       | STRING  | NO        | "true" or "false". default "false".                                                                                                    |
| price            | DECIMAL | NO        |                                                                                                                                        |
| newClientOrderId | STRING  | NO        | A unique id among open orders. Automatically generated if not sent. Can only be string following the rule:`^[\.A-Z\:/a-z0-9_-]{1,36}$` |
| stopPrice        | DECIMAL | NO        | Used with`STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.                                                                |
| activationPrice  | DECIMAL | NO        | Used with`TRAILING_STOP_MARKET` orders, default as the latest price(supporting different `workingType`)                                |
| callbackRate     | DECIMAL | NO        | Used with`TRAILING_STOP_MARKET` orders, min 0.1, max 4 where 1 for 1%                                                                  |
| workingType      | ENUM    | NO        | stopPrice triggered by: "MARK_PRICE", "CONTRACT_PRICE". Default "CONTRACT_PRICE"                                                       |
| priceProtect     | STRING  | NO        | "TRUE" or "FALSE", default "FALSE". Used with`STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.                            |
| newOrderRespType | ENUM    | NO        | "ACK", "RESULT", default "ACK"                                                                                                         |

* Paremeter rules are same with `New Order`
* Batch orders are processed concurrently, and the order of matching is not guaranteed.
* The order of returned contents for batch orders is the same as the order of the order list.


## Transfer Between Futures And Spot (USER_DATA)

> **Response:**

```javascript
{
    "tranId": 21841, //transaction id
    "status": "SUCCESS" //status
}
```

``
POST /fapi/v3/asset/wallet/transfer  (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	amount
asset |	STRING | 	YES |	asset
clientTranId |	STRING | 	YES |	transaction id 
kindType |	STRING | 	YES |	kindType
timestamp	| LONG | YES	|	timestamp

Notes:

* kindType can take the following values:
     FUTURE_SPOT  (futures converted to spot)
	 SPOT_FUTURE  (spot converted to futures)


## Query Order (USER_DATA)

> **Response:**

```javascript
{
  	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected
}
```

``GET /fapi/v1/order (HMAC SHA256)``

Check an order's status.

**Weight:**
1

* These orders will not be found:
  * order status is `CANCELED` or `EXPIRED`, **AND**
  * order has NO filled trade, **AND**
  * created time + 7 days < current time

**Parameters:**

| Name              | Type   | Mandatory | Description |
| ----------------- | ------ | --------- | ----------- |
| symbol            | STRING | YES       |             |
| orderId           | LONG   | NO        |             |
| origClientOrderId | STRING | NO        |             |
| recvWindow        | LONG   | NO        |             |
| timestamp         | LONG   | YES       |             |

Notes:

* Either `orderId` or `origClientOrderId` must be sent.

## Cancel Order (TRADE)

> **Response:**

```javascript
{
 	"clientOrderId": "myOrder1",
 	"cumQty": "0",
 	"cumQuote": "0",
 	"executedQty": "0",
 	"orderId": 283194212,
 	"origQty": "11",
 	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "CANCELED",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
 	"updateTime": 1571110484038,
 	"workingType": "CONTRACT_PRICE",
 	"priceProtect": false            // if conditional order trigger is protected
}
```

``DELETE /fapi/v1/order  (HMAC SHA256)``

Cancel an active order.

**Weight:**
1

**Parameters:**

| Name              | Type   | Mandatory | Description |
| ----------------- | ------ | --------- | ----------- |
| symbol            | STRING | YES       |             |
| orderId           | LONG   | NO        |             |
| origClientOrderId | STRING | NO        |             |
| recvWindow        | LONG   | NO        |             |
| timestamp         | LONG   | YES       |             |

Either `orderId` or `origClientOrderId` must be sent.

## Cancel All Open Orders (TRADE)

> **Response:**

```javascript
{
	"code": "200", 
	"msg": "The operation of cancel all open order is done."
}
```

``DELETE /fapi/v1/allOpenOrders  (HMAC SHA256)``

**Weight:**
1

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | YES       |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

## Cancel Multiple Orders (TRADE)

> **Response:**

```javascript
[
	{
	 	"clientOrderId": "myOrder1",
	 	"cumQty": "0",
	 	"cumQuote": "0",
	 	"executedQty": "0",
	 	"orderId": 283194212,
	 	"origQty": "11",
	 	"origType": "TRAILING_STOP_MARKET",
  		"price": "0",
  		"reduceOnly": false,
  		"side": "BUY",
  		"positionSide": "SHORT",
  		"status": "CANCELED",
  		"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  		"closePosition": false,   // if Close-All
  		"symbol": "BTCUSDT",
  		"timeInForce": "GTC",
  		"type": "TRAILING_STOP_MARKET",
  		"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  		"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
	 	"updateTime": 1571110484038,
	 	"workingType": "CONTRACT_PRICE",
	 	"priceProtect": false            // if conditional order trigger is protected
	},
	{
		"code": -2011,
		"msg": "Unknown order sent."
	}
]
```

``DELETE /fapi/v1/batchOrders  (HMAC SHA256)``

**Weight:**
1

**Parameters:**

| Name                  | Type           | Mandatory | Description                                                                                     |
| --------------------- | -------------- | --------- | ----------------------------------------------------------------------------------------------- |
| symbol                | STRING         | YES       |                                                                                                 |
| orderIdList           | LIST\   | NO        | max length 10 e.g. [1234567,2345678]                                                      |
| origClientOrderIdList | LIST\ | NO        | max length 10 e.g. ["my_id_1","my_id_2"], encode the double quotes. No space after comma. |
| recvWindow            | LONG           | NO        |                                                                                                 |
| timestamp             | LONG           | YES       |                                                                                                 |

Either `orderIdList` or `origClientOrderIdList ` must be sent.

## Auto-Cancel All Open Orders (TRADE)

> **Response:**

```javascript
{
	"symbol": "BTCUSDT", 
	"countdownTime": "100000"
}
```

Cancel all open orders of the specified symbol at the end of the specified countdown.

``POST /fapi/v1/countdownCancelAll  (HMAC SHA256)``

**Weight:**
10

**Parameters:**

| Name          | Type   | Mandatory | Description                                              |
| ------------- | ------ | --------- | -------------------------------------------------------- |
| symbol        | STRING | YES       |                                                          |
| countdownTime | LONG   | YES       | countdown time, 1000 for 1 second. 0 to cancel the timer |
| recvWindow    | LONG   | NO        |                                                          |
| timestamp     | LONG   | YES       |                                                          |

* The endpoint should be called repeatedly as heartbeats so that the existing countdown time can be canceled and replaced by a new one.
* Example usage:
  Call this endpoint at 30s intervals with an countdownTime of 120000 (120s).
  If this endpoint is not called within 120 seconds, all your orders of the specified symbol will be automatically canceled.
  If this endpoint is called with an countdownTime of 0, the countdown timer will be stopped.
* The system will check all countdowns **approximately every 10 milliseconds**, so please note that sufficient redundancy should be considered when using this function. We do not recommend setting the countdown time to be too precise or too small.

## Query Current Open Order (USER_DATA)

> **Response:**

```javascript
{
  	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",	
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",	
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   			// if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order	
  	"updateTime": 1579276756075,
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected
}
```

``GET /fapi/v1/openOrder  (HMAC SHA256)``

**Weight:** 1

**Parameters:**

| Name              | Type   | Mandatory | Description |
| ----------------- | ------ | --------- | ----------- |
| symbol            | STRING | YES       |             |
| orderId           | LONG   | NO        |             |
| origClientOrderId | STRING | NO        |             |
| recvWindow        | LONG   | NO        |             |
| timestamp         | LONG   | YES       |             |

* Either`orderId` or `origClientOrderId` must be sent
* If the queried order has been filled or cancelled, the error message "Order does not exist" will be returned.

## Current All Open Orders (USER_DATA)

> **Response:**

```javascript
[
  {
  	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected
  }
]
```

``GET /fapi/v1/openOrders  (HMAC SHA256)``

Get all open orders on a symbol. **Careful** when accessing this with no symbol.

**Weight:**
1 for a single symbol; **40** when the symbol parameter is omitted

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | NO        |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

* If the symbol is not sent, orders for all symbols will be returned in an array.

## All Orders (USER_DATA)

> **Response:**

```javascript
[
  {
   	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected
  }
]
```

``GET /fapi/v1/allOrders (HMAC SHA256)``

Get all account orders; active, canceled, or filled.

* These orders will not be found:
  * order status is `CANCELED` or `EXPIRED`, **AND**
  * order has NO filled trade, **AND**
  * created time + 7 days < current time

**Weight:**
5

**Parameters:**

| Name       | Type   | Mandatory | Description            |
| ---------- | ------ | --------- | ---------------------- |
| symbol     | STRING | YES       |                        |
| orderId    | LONG   | NO        |                        |
| startTime  | LONG   | NO        |                        |
| endTime    | LONG   | NO        |                        |
| limit      | INT    | NO        | Default 500; max 1000. |
| recvWindow | LONG   | NO        |                        |
| timestamp  | LONG   | YES       |                        |

**Notes:**

* If `orderId` is set, it will get orders >= that `orderId`. Otherwise most recent orders are returned.
* The query time period must be less then 7 days( default as the recent 7 days).

## Futures Account Balance v3 (USER_DATA)

> **Response:**

```javascript
[
 	{
 		"accountAlias": "SgsR",    // unique account code
 		"asset": "USDT",  	// asset name
 		"balance": "122607.35137903", // wallet balance
 		"crossWalletBalance": "23.72469206", // crossed wallet balance
  		"crossUnPnl": "0.00000000"  // unrealized profit of crossed positions
  		"availableBalance": "23.72469206",       // available balance
  		"maxWithdrawAmount": "23.72469206",     // maximum amount for transfer out
  		"marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
  		"updateTime": 1617939110373
	}
]
```

``GET /fapi/v3/balance (HMAC SHA256)``

**Weight:**
5

**Parameters:**

| Name       | Type | Mandatory | Description |
| ---------- | ---- | --------- | ----------- |
| recvWindow | LONG | NO        |             |
| timestamp  | LONG | YES       |             |

## Account Information v3 (USER_DATA)

> **Response:**

```javascript
{
	"feeTier": 0,  		// account commisssion tier 
 	"canTrade": true,  	// if can trade
 	"canDeposit": true,  	// if can transfer in asset
 	"canWithdraw": true, 	// if can transfer out asset
 	"updateTime": 0,
 	"totalInitialMargin": "0.00000000",    // total initial margin required with current mark price (useless with isolated positions), only for USDT asset
 	"totalMaintMargin": "0.00000000",  	  // total maintenance margin required, only for USDT asset
 	"totalWalletBalance": "23.72469206",     // total wallet balance, only for USDT asset
 	"totalUnrealizedProfit": "0.00000000",   // total unrealized profit, only for USDT asset
 	"totalMarginBalance": "23.72469206",     // total margin balance, only for USDT asset
 	"totalPositionInitialMargin": "0.00000000",    // initial margin required for positions with current mark price, only for USDT asset
 	"totalOpenOrderInitialMargin": "0.00000000",   // initial margin required for open orders with current mark price, only for USDT asset
 	"totalCrossWalletBalance": "23.72469206",      // crossed wallet balance, only for USDT asset
 	"totalCrossUnPnl": "0.00000000",	  // unrealized profit of crossed positions, only for USDT asset
 	"availableBalance": "23.72469206",       // available balance, only for USDT asset
 	"maxWithdrawAmount": "23.72469206"     // maximum amount for transfer out, only for USDT asset
 	"assets": [
 		{
 			"asset": "USDT",			// asset name
		   	"walletBalance": "23.72469206",      // wallet balance
		   	"unrealizedProfit": "0.00000000",    // unrealized profit
		   	"marginBalance": "23.72469206",      // margin balance
		   	"maintMargin": "0.00000000",	    // maintenance margin required
		   	"initialMargin": "0.00000000",    // total initial margin required with current mark price 
		   	"positionInitialMargin": "0.00000000",    //initial margin required for positions with current mark price
		   	"openOrderInitialMargin": "0.00000000",   // initial margin required for open orders with current mark price
		   	"crossWalletBalance": "23.72469206",      // crossed wallet balance
		   	"crossUnPnl": "0.00000000"       // unrealized profit of crossed positions
		   	"availableBalance": "23.72469206",       // available balance
		   	"maxWithdrawAmount": "23.72469206",     // maximum amount for transfer out
		   	"marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
		   	"updateTime": 1625474304765 // last update time 
		},
		{
 			"asset": "BUSD",			// asset name
		   	"walletBalance": "103.12345678",      // wallet balance
		   	"unrealizedProfit": "0.00000000",    // unrealized profit
		   	"marginBalance": "103.12345678",      // margin balance
		   	"maintMargin": "0.00000000",	    // maintenance margin required
		   	"initialMargin": "0.00000000",    // total initial margin required with current mark price 
		   	"positionInitialMargin": "0.00000000",    //initial margin required for positions with current mark price
		   	"openOrderInitialMargin": "0.00000000",   // initial margin required for open orders with current mark price
		   	"crossWalletBalance": "103.12345678",      // crossed wallet balance
		   	"crossUnPnl": "0.00000000"       // unrealized profit of crossed positions
		   	"availableBalance": "103.12345678",       // available balance
		   	"maxWithdrawAmount": "103.12345678",     // maximum amount for transfer out
		   	"marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
		   	"updateTime": 1625474304765 // last update time
		}
	],
 	"positions": [  // positions of all symbols in the market are returned
 		// only "BOTH" positions will be returned with One-way mode
 		// only "LONG" and "SHORT" positions will be returned with Hedge mode
 		{
		 	"symbol": "BTCUSDT",  	// symbol name
		   	"initialMargin": "0",	// initial margin required with current mark price 
		   	"maintMargin": "0",		// maintenance margin required
		   	"unrealizedProfit": "0.00000000",  // unrealized profit
		   	"positionInitialMargin": "0",      // initial margin required for positions with current mark price
		   	"openOrderInitialMargin": "0",     // initial margin required for open orders with current mark price
		   	"leverage": "100",		// current initial leverage
		   	"isolated": true,  		// if the position is isolated
		   	"entryPrice": "0.00000",  	// average entry price
		   	"maxNotional": "250000",  	// maximum available notional with current leverage
		   	"positionSide": "BOTH",  	// position side
		   	"positionAmt": "0",			// position amount
		   	"updateTime": 0           // last update time
		}
  	]
}
```

``GET /fapi/v3/account (HMAC SHA256)``

Get current account information.

**Weight:**
5

**Parameters:**

| Name       | Type | Mandatory | Description |
| ---------- | ---- | --------- | ----------- |
| recvWindow | LONG | NO        |             |
| timestamp  | LONG | YES       |             |

## Change Initial Leverage (TRADE)

> **Response:**

```javascript
{
 	"leverage": 21,
 	"maxNotionalValue": "1000000",
 	"symbol": "BTCUSDT"
}
```

``POST /fapi/v1/leverage (HMAC SHA256)``

Change user's initial leverage of specific symbol market.

**Weight:**
1

**Parameters:**

| Name       | Type   | Mandatory | Description                                |
| ---------- | ------ | --------- | ------------------------------------------ |
| symbol     | STRING | YES       |                                            |
| leverage   | INT    | YES       | target initial leverage: int from 1 to 125 |
| recvWindow | LONG   | NO        |                                            |
| timestamp  | LONG   | YES       |                                            |

## Change Margin Type (TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``POST /fapi/v1/marginType (HMAC SHA256)``

**Weight:**
1

**Parameters:**

| Name       | Type   | Mandatory | Description       |
| ---------- | ------ | --------- | ----------------- |
| symbol     | STRING | YES       |                   |
| marginType | ENUM   | YES       | ISOLATED, CROSSED |
| recvWindow | LONG   | NO        |                   |
| timestamp  | LONG   | YES       |                   |

## Modify Isolated Position Margin (TRADE)

> **Response:**

```javascript
{
	"amount": 100.0,
  	"code": 200,
  	"msg": "Successfully modify position margin.",
  	"type": 1
}
```

``POST /fapi/v1/positionMargin (HMAC SHA256)``

**Weight:**
1

**Parameters:**

| Name         | Type    | Mandatory | Description                                                                                         |
| ------------ | ------- | --------- | --------------------------------------------------------------------------------------------------- |
| symbol       | STRING  | YES       |                                                                                                     |
| positionSide | ENUM    | NO        | Default`BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent with Hedge Mode. |
| amount       | DECIMAL | YES       |                                                                                                     |
| type         | INT     | YES       | 1: Add position margin，2: Reduce position margin                                                   |
| recvWindow   | LONG    | NO        |                                                                                                     |
| timestamp    | LONG    | YES       |                                                                                                     |

* Only for isolated symbol

## Get Position Margin Change History (TRADE)

> **Response:**

```javascript
[
	{
		"amount": "23.36332311",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047897183,
	  	"type": 1,
	  	"positionSide": "BOTH"
	},
	{
		"amount": "100",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047900425,
	  	"type": 1,
	  	"positionSide": "LONG"
	}
]
```

``GET /fapi/v1/positionMargin/history (HMAC SHA256)``

**Weight:**
1

**Parameters:**

| Name       | Type   | Mandatory | Description                                       |
| ---------- | ------ | --------- | ------------------------------------------------- |
| symbol     | STRING | YES       |                                                   |
| type       | INT    | NO        | 1: Add position margin，2: Reduce position margin |
| startTime  | LONG   | NO        |                                                   |
| endTime    | LONG   | NO        |                                                   |
| limit      | INT    | NO        | Default: 500                                      |
| recvWindow | LONG   | NO        |                                                   |
| timestamp  | LONG   | YES       |                                                   |

## Position Information v3 (USER_DATA)

> **Response:**

> For One-way position mode:

```javascript
[
  	{
  		"entryPrice": "0.00000",
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "0.00000000",
  		"leverage": "10", 
  		"liquidationPrice": "0", 
  		"markPrice": "6679.50671178",
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "0.000", 
  		"symbol": "BTCUSDT", 
  		"unRealizedProfit": "0.00000000", 
  		"positionSide": "BOTH",
  		"updateTime": 0
  	}
]
```

> For Hedge position mode:

```javascript
[
  	{
  		"entryPrice": "6563.66500", 
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "15517.54150468",
  		"leverage": "10",
  		"liquidationPrice": "5930.78",
  		"markPrice": "6679.50671178",
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "20.000", 
  		"symbol": "BTCUSDT", 
  		"unRealizedProfit": "2316.83423560"
  		"positionSide": "LONG", 
  		"updateTime": 1625474304765
  	},
  	{
  		"entryPrice": "0.00000",
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "5413.95799991", 
  		"leverage": "10", 
  		"liquidationPrice": "7189.95", 
  		"markPrice": "6679.50671178",
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "-10.000", 
  		"symbol": "BTCUSDT",
  		"unRealizedProfit": "-1156.46711780" 
  		"positionSide": "SHORT",
  		"updateTime": 0
  	}
]
```

``GET /fapi/v3/positionRisk (HMAC SHA256)``

Get current position information.

**Weight:**
5

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | NO        |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

**Note**
Please use with user data stream `ACCOUNT_UPDATE` to meet your timeliness and accuracy needs.

## Account Trade List (USER_DATA)

> **Response:**

```javascript
[
  {
  	"buyer": false,
  	"commission": "-0.07819010",
  	"commissionAsset": "USDT",
  	"id": 698759,
  	"maker": false,
  	"orderId": 25851813,
  	"price": "7819.01",
  	"qty": "0.002",
  	"quoteQty": "15.63802",
  	"realizedPnl": "-0.91539999",
  	"side": "SELL",
  	"positionSide": "SHORT",
  	"symbol": "BTCUSDT",
  	"time": 1569514978020
  }
]
```

``GET /fapi/v1/userTrades  (HMAC SHA256)``

Get trades for a specific account and symbol.

**Weight:**
5

**Parameters:**

| Name       | Type   | Mandatory | Description                                              |
| ---------- | ------ | --------- | -------------------------------------------------------- |
| symbol     | STRING | YES       |                                                          |
| startTime  | LONG   | NO        |                                                          |
| endTime    | LONG   | NO        |                                                          |
| fromId     | LONG   | NO        | Trade id to fetch from. Default gets most recent trades. |
| limit      | INT    | NO        | Default 500; max 1000.                                   |
| recvWindow | LONG   | NO        |                                                          |
| timestamp  | LONG   | YES       |                                                          |

* If `startTime` and `endTime` are both not sent, then the last 7 days' data will be returned.
* The time between `startTime` and `endTime` cannot be longer than 7 days.
* The parameter `fromId` cannot be sent with `startTime` or `endTime`.

## Get Income History(USER_DATA)

> **Response:**

```javascript
[
	{
    	"symbol": "",					// trade symbol, if existing
    	"incomeType": "TRANSFER",	// income type
    	"income": "-0.37500000",  // income amount
    	"asset": "USDT",				// income asset
    	"info":"TRANSFER",			// extra information
    	"time": 1570608000000,
    	"tranId":"9689322392",		// transaction id
    	"tradeId":""					// trade id, if existing
	},
	{
   		"symbol": "BTCUSDT",
    	"incomeType": "COMMISSION", 
    	"income": "-0.01000000",
    	"asset": "USDT",
    	"info":"COMMISSION",
    	"time": 1570636800000,
    	"tranId":"9689322392",
    	"tradeId":"2059192"
	}
]
```

``GET /fapi/v1/income (HMAC SHA256)``

**Weight:**
30

**Parameters:**

| Name       | Type   | Mandatory | Description                                                                                                                      |
| ---------- | ------ | --------- | -------------------------------------------------------------------------------------------------------------------------------- |
| symbol     | STRING | NO        |                                                                                                                                  |
| incomeType | STRING | NO        | "TRANSFER"，"WELCOME_BONUS", "REALIZED_PNL"，"FUNDING_FEE", "COMMISSION", "INSURANCE_CLEAR", and "MARKET_MERCHANT_RETURN_REWARD" |
| startTime  | LONG   | NO        | Timestamp in ms to get funding from INCLUSIVE.                                                                                   |
| endTime    | LONG   | NO        | Timestamp in ms to get funding until INCLUSIVE.                                                                                  |
| limit      | INT    | NO        | Default 100; max 1000                                                                                                            |
| recvWindow | LONG   | NO        |                                                                                                                                  |
| timestamp  | LONG   | YES       |                                                                                                                                  |

* If neither `startTime` nor `endTime` is sent, the recent 7-day data will be returned.
* If `incomeType ` is not sent, all kinds of flow will be returned
* "trandId" is unique in the same incomeType for a user

## Notional and Leverage Brackets (USER_DATA)

> **Response:**

```javascript
[
    {
        "symbol": "ETHUSDT",
        "brackets": [
            {
                "bracket": 1,   // Notional bracket
                "initialLeverage": 75,  // Max initial leverage for this bracket
                "notionalCap": 10000,  // Cap notional of this bracket
                "notionalFloor": 0,  // Notional threshold of this bracket 
                "maintMarginRatio": 0.0065, // Maintenance ratio for this bracket
                "cum":0 // Auxiliary number for quick calculation 
     
            },
        ]
    }
]
```

> **OR** (if symbol sent)

```javascript
{
    "symbol": "ETHUSDT",
    "brackets": [
        {
            "bracket": 1,
            "initialLeverage": 75,
            "notionalCap": 10000,
            "notionalFloor": 0,
            "maintMarginRatio": 0.0065,
            "cum":0
        },
    ]
}
```

``GET /fapi/v1/leverageBracket``

**Weight:** 1

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | NO        |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

## Position ADL Quantile Estimation (USER_DATA)

> **Response:**

```javascript
[
	{
		"symbol": "ETHUSDT", 
		"adlQuantile": 
			{
				// if the positions of the symbol are crossed margined in Hedge Mode, "LONG" and "SHORT" will be returned a same quantile value, and "HEDGE" will be returned instead of "BOTH".
				"LONG": 3,  
				"SHORT": 3, 
				"HEDGE": 0   // only a sign, ignore the value
			}
		},
 	{
 		"symbol": "BTCUSDT", 
 		"adlQuantile": 
 			{
 				// for positions of the symbol are in One-way Mode or isolated margined in Hedge Mode
 				"LONG": 1, 	// adl quantile for "LONG" position in hedge mode
 				"SHORT": 2, 	// adl qauntile for "SHORT" position in hedge mode
 				"BOTH": 0		// adl qunatile for position in one-way mode
 			}
 	}
 ]
```

``GET /fapi/v1/adlQuantile``

**Weight:** 5

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | NO        |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

* Values update every 30s.
* Values 0, 1, 2, 3, 4 shows the queue position and possibility of ADL from low to high.
* For positions of the symbol are in One-way Mode or isolated margined in Hedge Mode, "LONG", "SHORT", and "BOTH" will be returned to show the positions' adl quantiles of different position sides.
* If the positions of the symbol are crossed margined in Hedge Mode:
  
  * "HEDGE" as a sign will be returned instead of "BOTH";
  * A same value caculated on unrealized pnls on long and short sides' positions will be shown for "LONG" and "SHORT" when there are positions in both of long and short sides.

## User's Force Orders (USER_DATA)

> **Response:**

```javascript
[
  {
  	"orderId": 6071832819, 
  	"symbol": "BTCUSDT", 
  	"status": "FILLED", 
  	"clientOrderId": "autoclose-1596107620040000020", 
  	"price": "10871.09", 
  	"avgPrice": "10913.21000", 
  	"origQty": "0.001", 
  	"executedQty": "0.001", 
  	"cumQuote": "10.91321", 
  	"timeInForce": "IOC", 
  	"type": "LIMIT", 
  	"reduceOnly": false, 
  	"closePosition": false, 
  	"side": "SELL", 
  	"positionSide": "BOTH", 
  	"stopPrice": "0", 
  	"workingType": "CONTRACT_PRICE", 
  	"origType": "LIMIT", 
  	"time": 1596107620044, 
  	"updateTime": 1596107620087
  }
  {
   	"orderId": 6072734303, 
   	"symbol": "BTCUSDT", 
   	"status": "FILLED", 
   	"clientOrderId": "adl_autoclose", 
   	"price": "11023.14", 
   	"avgPrice": "10979.82000", 
   	"origQty": "0.001", 
   	"executedQty": "0.001", 
   	"cumQuote": "10.97982", 
   	"timeInForce": "GTC", 
   	"type": "LIMIT", 
   	"reduceOnly": false, 
   	"closePosition": false, 
   	"side": "BUY", 
   	"positionSide": "SHORT", 
   	"stopPrice": "0", 
   	"workingType": "CONTRACT_PRICE", 
   	"origType": "LIMIT", 
   	"time": 1596110725059, 
   	"updateTime": 1596110725071
  }
]
```

``GET /fapi/v1/forceOrders``

**Weight:** 20 with symbol, 50 without symbol

**Parameters:**

| Name          | Type   | Mandatory | Description                                                 |
| ------------- | ------ | --------- | ----------------------------------------------------------- |
| symbol        | STRING | NO        |                                                             |
| autoCloseType | ENUM   | NO        | "LIQUIDATION" for liquidation orders, "ADL" for ADL orders. |
| startTime     | LONG   | NO        |                                                             |
| endTime       | LONG   | NO        |                                                             |
| limit         | INT    | NO        | Default 50; max 100.                                        |
| recvWindow    | LONG   | NO        |                                                             |
| timestamp     | LONG   | YES       |                                                             |

* If "autoCloseType" is not sent, orders with both of the types will be returned
* If "startTime" is not sent, data within 7 days before "endTime" can be queried

## User Commission Rate (USER_DATA)

> **Response:**

```javascript
{
	"symbol": "BTCUSDT",
  	"makerCommissionRate": "0.0002",  // 0.02%
  	"takerCommissionRate": "0.0004"   // 0.04%
}
```

``GET /fapi/v1/commissionRate (HMAC SHA256)``

**Weight:**
20

**Parameters:**

| Name       | Type   | Mandatory | Description |
| ---------- | ------ | --------- | ----------- |
| symbol     | STRING | YES       |             |
| recvWindow | LONG   | NO        |             |
| timestamp  | LONG   | YES       |             |

# User Data Streams

* The base API endpoint is: **https://fapi.asterdex.com**
* A User Data Stream `listenKey` is valid for 60 minutes after creation.
* Doing a `PUT` on a `listenKey` will extend its validity for 60 minutes.
* Doing a `DELETE` on a `listenKey` will close the stream and invalidate the `listenKey`.
* Doing a `POST` on an account with an active `listenKey` will return the currently active `listenKey` and extend its validity for 60 minutes.
* The baseurl for websocket is **wss://fstream.asterdex.com**
* User Data Streams are accessed at **/ws/\<listenKey\>**
* User data stream payloads are **not guaranteed** to be in order during heavy periods; **make sure to order your updates using E**
* A single connection to **fstream.asterdex.com** is only valid for 24 hours; expect to be disconnected at the 24 hour mark

## Start User Data Stream (USER_STREAM)

> **Response:**

```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

``POST /fapi/v1/listenKey``

Start a new user data stream. The stream will close after 60 minutes unless a keepalive is sent. If the account has an active `listenKey`, that `listenKey` will be returned and its validity will be extended for 60 minutes.

**Weight:**
1

**Parameters:**

None

## Keepalive User Data Stream (USER_STREAM)

> **Response:**

```javascript
{}
```

``PUT /fapi/v1/listenKey``

Keepalive a user data stream to prevent a time out. User data streams will close after 60 minutes. It's recommended to send a ping about every 60 minutes.

**Weight:**
1

**Parameters:**

None

## Close User Data Stream (USER_STREAM)

> **Response:**

```javascript
{}
```

``DELETE /fapi/v1/listenKey``

Close out a user data stream.

**Weight:**
1

**Parameters:**

None

## Event: User Data Stream Expired

> **Payload:**

```javascript
{
	'e': 'listenKeyExpired',      // event type
	'E': 1576653824250				// event time
}
```

When the `listenKey` used for the user data stream turns expired, this event will be pushed.

**Notice:**

* This event is not related to the websocket disconnection.
* This event will be received only when a valid `listenKey` in connection got expired.
* No more user data event will be updated after this event received until a new valid `listenKey` used.

## Event: Margin Call

> **Payload:**

```javascript
{
    "e":"MARGIN_CALL",    	// Event Type
    "E":1587727187525,		// Event Time
    "cw":"3.16812045",		// Cross Wallet Balance. Only pushed with crossed position margin call
    "p":[					// Position(s) of Margin Call
      {
        "s":"ETHUSDT",		// Symbol
        "ps":"LONG",		// Position Side
        "pa":"1.327",		// Position Amount
        "mt":"CROSSED",		// Margin Type
        "iw":"0",			// Isolated Wallet (if isolated position)
        "mp":"187.17127",	// Mark Price
        "up":"-1.166074",	// Unrealized PnL
        "mm":"1.614445"		// Maintenance Margin Required
      }
    ]
}
```

* When the user's position risk ratio is too high, this stream will be pushed.
* This message is only used as risk guidance information and is not recommended for investment strategies.
* In the case of a highly volatile market, there may be the possibility that the user's position has been liquidated at the same time when this stream is pushed out.

## Event: Balance and Position Update

> **Payload:**

```javascript
{
  "e": "ACCOUNT_UPDATE",				// Event Type
  "E": 1564745798939,            		// Event Time
  "T": 1564745798938 ,           		// Transaction
  "a":                          		// Update Data
    {
      "m":"ORDER",						// Event reason type
      "B":[                     		// Balances
        {
          "a":"USDT",           		// Asset
          "wb":"122624.12345678",    	// Wallet Balance
          "cw":"100.12345678",			// Cross Wallet Balance
          "bc":"50.12345678"			// Balance Change except PnL and Commission
        },
        {
          "a":"BUSD",   
          "wb":"1.00000000",
          "cw":"0.00000000",   
          "bc":"-49.12345678"
        }
      ],
      "P":[
        {
          "s":"BTCUSDT",          	// Symbol
          "pa":"0",               	// Position Amount
          "ep":"0.00000",            // Entry Price
          "cr":"200",             	// (Pre-fee) Accumulated Realized
          "up":"0",						// Unrealized PnL
          "mt":"isolated",				// Margin Type
          "iw":"0.00000000",			// Isolated Wallet (if isolated position)
          "ps":"BOTH"					// Position Side
        }，
        {
        	"s":"BTCUSDT",
        	"pa":"20",
        	"ep":"6563.66500",
        	"cr":"0",
        	"up":"2850.21200",
        	"mt":"isolated",
        	"iw":"13200.70726908",
        	"ps":"LONG"
      	 },
        {
        	"s":"BTCUSDT",
        	"pa":"-10",
        	"ep":"6563.86000",
        	"cr":"-45.04000000",
        	"up":"-1423.15600",
        	"mt":"isolated",
        	"iw":"6570.42511771",
        	"ps":"SHORT"
        }
      ]
    }
}
```

Event type is `ACCOUNT_UPDATE`.

* When balance or position get updated, this event will be pushed.
  
  * `ACCOUNT_UPDATE` will be pushed only when update happens on user's account, including changes on balances, positions, or margin type.
  * Unfilled orders or cancelled orders will not make the event `ACCOUNT_UPDATE` pushed, since there's no change on positions.
  * Only positions of symbols with non-zero isolatd wallet or non-zero position amount will be pushed in the "position" part of the event `ACCOUNT_UPDATE` when any position changes.
* When "FUNDING FEE" changes to the user's balance, the event will be pushed with the brief message:
  
  * When "FUNDING FEE" occurs in a **crossed position**, `ACCOUNT_UPDATE` will be pushed with only the balance `B`(including the "FUNDING FEE" asset only), without any position `P` message.
  * When "FUNDING FEE" occurs in an **isolated position**, `ACCOUNT_UPDATE` will be pushed with only the balance `B`(including the "FUNDING FEE" asset only) and the relative position message `P`( including the isolated position on which the "FUNDING FEE" occurs only, without any other position message).
* The field "m" represents the reason type for the event and may shows the following possible types:
  
  * DEPOSIT
  * WITHDRAW
  * ORDER
  * FUNDING_FEE
  * WITHDRAW_REJECT
  * ADJUSTMENT
  * INSURANCE_CLEAR
  * ADMIN_DEPOSIT
  * ADMIN_WITHDRAW
  * MARGIN_TRANSFER
  * MARGIN_TYPE_CHANGE
  * ASSET_TRANSFER
  * OPTIONS_PREMIUM_FEE
  * OPTIONS_SETTLE_PROFIT
  * AUTO_EXCHANGE
* The field "bc" represents the balance change except for PnL and commission.

## Event: Order Update

> **Payload:**

```javascript
{
  
  "e":"ORDER_TRADE_UPDATE",		// Event Type
  "E":1568879465651,			// Event Time
  "T":1568879465650,			// Transaction Time
  "o":{			
    "s":"BTCUSDT",				// Symbol
    "c":"TEST",					// Client Order Id
      // special client order id:
      // starts with "autoclose-": liquidation order
      // "adl_autoclose": ADL auto close order
    "S":"SELL",					// Side
    "o":"TRAILING_STOP_MARKET",	// Order Type
    "f":"GTC",					// Time in Force
    "q":"0.001",				// Original Quantity
    "p":"0",					// Original Price
    "ap":"0",					// Average Price
    "sp":"7103.04",				// Stop Price. Please ignore with TRAILING_STOP_MARKET order
    "x":"NEW",					// Execution Type
    "X":"NEW",					// Order Status
    "i":8886774,				// Order Id
    "l":"0",					// Order Last Filled Quantity
    "z":"0",					// Order Filled Accumulated Quantity
    "L":"0",					// Last Filled Price
    "N":"USDT",            	// Commission Asset, will not push if no commission
    "n":"0",               	// Commission, will not push if no commission
    "T":1568879465651,			// Order Trade Time
    "t":0,			        	// Trade Id
    "b":"0",			    	// Bids Notional
    "a":"9.91",					// Ask Notional
    "m":false,					// Is this trade the maker side?
    "R":false,					// Is this reduce only
    "wt":"CONTRACT_PRICE", 		// Stop Price Working Type
    "ot":"TRAILING_STOP_MARKET",	// Original Order Type
    "ps":"LONG",						// Position Side
    "cp":false,						// If Close-All, pushed with conditional order
    "AP":"7476.89",				// Activation Price, only puhed with TRAILING_STOP_MARKET order
    "cr":"5.0",					// Callback Rate, only puhed with TRAILING_STOP_MARKET order
    "rp":"0"							// Realized Profit of the trade
  }
  
}
```

When new order created, order status changed will push such event.
event type is `ORDER_TRADE_UPDATE`.

**Side**

* BUY
* SELL

**Order Type**

* MARKET
* LIMIT
* STOP
* TAKE_PROFIT
* LIQUIDATION

**Execution Type**

* NEW
* CANCELED
* CALCULATED		 - Liquidation Execution
* EXPIRED
* TRADE

**Order Status**

* NEW
* PARTIALLY_FILLED
* FILLED
* CANCELED
* EXPIRED
* NEW_INSURANCE     - Liquidation with Insurance Fund
* NEW_ADL				- Counterparty Liquidation`

**Time in force**

* GTC
* IOC
* FOK
* GTX

**Working Type**

* MARK_PRICE
* CONTRACT_PRICE

## Event: Account Configuration Update previous Leverage Update

> **Payload:**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
    "E":1611646737479,		           // Event Time
    "T":1611646737476,		           // Transaction Time
    "ac":{			
    "s":"BTCUSDT",					   // symbol
    "l":25						       // leverage
   
    }
}
```

> **Or**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
    "E":1611646737479,		           // Event Time
    "T":1611646737476,		           // Transaction Time
    "ai":{							   // User's Account Configuration
    "j":true						   // Multi-Assets Mode
    }
}
```

When the account configuration is changed, the event type will be pushed as `ACCOUNT_CONFIG_UPDATE`

When the leverage of a trade pair changes, the payload will contain the object `ac` to represent the account configuration of the trade pair, where `s` represents the specific trade pair and `l` represents the leverage

When the user Multi-Assets margin mode changes the payload will contain the object `ai` representing the user account configuration, where `j` represents the user Multi-Assets margin mode

# Error Codes

> Here is the error JSON payload:

```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

Errors consist of two parts: an error code and a message.
Codes are universal,but messages can vary.

## 10xx - General Server or Network issues

> -1000 UNKNOWN

* An unknown error occured while processing the request.

> -1001 DISCONNECTED

* Internal error; unable to process your request. Please try again.

> -1002 UNAUTHORIZED

* You are not authorized to execute this request.

> -1003 TOO_MANY_REQUESTS

* Too many requests queued.
* Too many requests; please use the websocket for live updates.
* Too many requests; current limit is %s requests per minute. Please use the websocket for live updates to avoid polling the API.
* Way too many requests; IP banned until %s. Please use the websocket for live updates to avoid bans.

> -1004 DUPLICATE_IP

* This IP is already on the white list

> -1005 NO_SUCH_IP

* No such IP has been white listed

> -1006 UNEXPECTED_RESP

* An unexpected response was received from the message bus. Execution status unknown.

> -1007 TIMEOUT

* Timeout waiting for response from backend server. Send status unknown; execution status unknown.

> -1010 ERROR_MSG_RECEIVED

* ERROR_MSG_RECEIVED.

> -1011 NON_WHITE_LIST

* This IP cannot access this route.

> -1013 INVALID_MESSAGE

* INVALID_MESSAGE.

> -1014 UNKNOWN_ORDER_COMPOSITION

* Unsupported order combination.

> -1015 TOO_MANY_ORDERS

* Too many new orders.
* Too many new orders; current limit is %s orders per %s.

> -1016 SERVICE_SHUTTING_DOWN

* This service is no longer available.

> -1020 UNSUPPORTED_OPERATION

* This operation is not supported.

> -1021 INVALID_TIMESTAMP

* Timestamp for this request is outside of the recvWindow.
* Timestamp for this request was 1000ms ahead of the server's time.

> -1022 INVALID_SIGNATURE

* Signature for this request is not valid.

> -1023 START_TIME_GREATER_THAN_END_TIME

* Start time is greater than end time.

## 11xx - Request issues

> -1100 ILLEGAL_CHARS

* Illegal characters found in a parameter.
* Illegal characters found in parameter '%s'; legal range is '%s'.

> -1101 TOO_MANY_PARAMETERS

* Too many parameters sent for this endpoint.
* Too many parameters; expected '%s' and received '%s'.
* Duplicate values for a parameter detected.

> -1102 MANDATORY_PARAM_EMPTY_OR_MALFORMED

* A mandatory parameter was not sent, was empty/null, or malformed.
* Mandatory parameter '%s' was not sent, was empty/null, or malformed.
* Param '%s' or '%s' must be sent, but both were empty/null!

> -1103 UNKNOWN_PARAM

* An unknown parameter was sent.

> -1104 UNREAD_PARAMETERS

* Not all sent parameters were read.
* Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.

> -1105 PARAM_EMPTY

* A parameter was empty.
* Parameter '%s' was empty.

> -1106 PARAM_NOT_REQUIRED

* A parameter was sent when not required.
* Parameter '%s' sent when not required.

> -1108 BAD_ASSET

* Invalid asset.

> -1109 BAD_ACCOUNT

* Invalid account.

> -1110 BAD_INSTRUMENT_TYPE

* Invalid symbolType.

> -1111 BAD_PRECISION

* Precision is over the maximum defined for this asset.

> -1112 NO_DEPTH

* No orders on book for symbol.

> -1113 WITHDRAW_NOT_NEGATIVE

* Withdrawal amount must be negative.

> -1114 TIF_NOT_REQUIRED

* TimeInForce parameter sent when not required.

> -1115 INVALID_TIF

* Invalid timeInForce.

> -1116 INVALID_ORDER_TYPE

* Invalid orderType.

> -1117 INVALID_SIDE

* Invalid side.

> -1118 EMPTY_NEW_CL_ORD_ID

* New client order ID was empty.

> -1119 EMPTY_ORG_CL_ORD_ID

* Original client order ID was empty.

> -1120 BAD_INTERVAL

* Invalid interval.

> -1121 BAD_SYMBOL

* Invalid symbol.

> -1125 INVALID_LISTEN_KEY

* This listenKey does not exist.

> -1127 MORE_THAN_XX_HOURS

* Lookup interval is too big.
* More than %s hours between startTime and endTime.

> -1128 OPTIONAL_PARAMS_BAD_COMBO

* Combination of optional parameters invalid.

> -1130 INVALID_PARAMETER

* Invalid data sent for a parameter.
* Data sent for parameter '%s' is not valid.

> -1136 INVALID_NEW_ORDER_RESP_TYPE

* Invalid newOrderRespType.

## 20xx - Processing Issues

> -2010 NEW_ORDER_REJECTED

* NEW_ORDER_REJECTED

> -2011 CANCEL_REJECTED

* CANCEL_REJECTED

> -2013 NO_SUCH_ORDER

* Order does not exist.

> -2014 BAD_API_KEY_FMT

* API-key format invalid.

> -2015 REJECTED_MBX_KEY

* Invalid API-key, IP, or permissions for action.

> -2016 NO_TRADING_WINDOW

* No trading window could be found for the symbol. Try ticker/24hrs instead.

> -2018 BALANCE_NOT_SUFFICIENT

* Balance is insufficient.

> -2019 MARGIN_NOT_SUFFICIEN

* Margin is insufficient.

> -2020 UNABLE_TO_FILL

* Unable to fill.

> -2021 ORDER_WOULD_IMMEDIATELY_TRIGGER

* Order would immediately trigger.

> -2022 REDUCE_ONLY_REJECT

* ReduceOnly Order is rejected.

> -2023 USER_IN_LIQUIDATION

* User in liquidation mode now.

> -2024 POSITION_NOT_SUFFICIENT

* Position is not sufficient.

> -2025 MAX_OPEN_ORDER_EXCEEDED

* Reach max open order limit.

> -2026 REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED

* This OrderType is not supported when reduceOnly.

> -2027 MAX_LEVERAGE_RATIO

* Exceeded the maximum allowable position at current leverage.

> -2028 MIN_LEVERAGE_RATIO

* Leverage is smaller than permitted: insufficient margin balance.

## 40xx - Filters and other Issues

> -4000 INVALID_ORDER_STATUS

* Invalid order status.

> -4001 PRICE_LESS_THAN_ZERO

* Price less than 0.

> -4002 PRICE_GREATER_THAN_MAX_PRICE

* Price greater than max price.

> -4003 QTY_LESS_THAN_ZERO

* Quantity less than zero.

> -4004 QTY_LESS_THAN_MIN_QTY

* Quantity less than min quantity.

> -4005 QTY_GREATER_THAN_MAX_QTY

* Quantity greater than max quantity.

> -4006 STOP_PRICE_LESS_THAN_ZERO

* Stop price less than zero.

> -4007 STOP_PRICE_GREATER_THAN_MAX_PRICE

* Stop price greater than max price.

> -4008 TICK_SIZE_LESS_THAN_ZERO

* Tick size less than zero.

> -4009 MAX_PRICE_LESS_THAN_MIN_PRICE

* Max price less than min price.

> -4010 MAX_QTY_LESS_THAN_MIN_QTY

* Max qty less than min qty.

> -4011 STEP_SIZE_LESS_THAN_ZERO

* Step size less than zero.

> -4012 MAX_NUM_ORDERS_LESS_THAN_ZERO

* Max mum orders less than zero.

> -4013 PRICE_LESS_THAN_MIN_PRICE

* Price less than min price.

> -4014 PRICE_NOT_INCREASED_BY_TICK_SIZE

* Price not increased by tick size.

> -4015 INVALID_CL_ORD_ID_LEN

* Client order id is not valid.
* Client order id length should not be more than 36 chars

> -4016 PRICE_HIGHTER_THAN_MULTIPLIER_UP

* Price is higher than mark price multiplier cap.

> -4017 MULTIPLIER_UP_LESS_THAN_ZERO

* Multiplier up less than zero.

> -4018 MULTIPLIER_DOWN_LESS_THAN_ZERO

* Multiplier down less than zero.

> -4019 COMPOSITE_SCALE_OVERFLOW

* Composite scale too large.

> -4020 TARGET_STRATEGY_INVALID

* Target strategy invalid for orderType '%s',reduceOnly '%b'.

> -4021 INVALID_DEPTH_LIMIT

* Invalid depth limit.
* '%s' is not valid depth limit.

> -4022 WRONG_MARKET_STATUS

* market status sent is not valid.

> -4023 QTY_NOT_INCREASED_BY_STEP_SIZE

* Qty not increased by step size.

> -4024 PRICE_LOWER_THAN_MULTIPLIER_DOWN

* Price is lower than mark price multiplier floor.

> -4025 MULTIPLIER_DECIMAL_LESS_THAN_ZERO

* Multiplier decimal less than zero.

> -4026 COMMISSION_INVALID

* Commission invalid.
* `%s` less than zero.
* `%s` absolute value greater than `%s`

> -4027 INVALID_ACCOUNT_TYPE

* Invalid account type.

> -4028 INVALID_LEVERAGE

* Invalid leverage
* Leverage `%s` is not valid
* Leverage `%s` already exist with `%s`

> -4029 INVALID_TICK_SIZE_PRECISION

* Tick size precision is invalid.

> -4030 INVALID_STEP_SIZE_PRECISION

* Step size precision is invalid.

> -4031 INVALID_WORKING_TYPE

* Invalid parameter working type
* Invalid parameter working type: `%s`

> -4032 EXCEED_MAX_CANCEL_ORDER_SIZE

* Exceed maximum cancel order size.
* Invalid parameter working type: `%s`

> -4033 INSURANCE_ACCOUNT_NOT_FOUND

* Insurance account not found.

> -4044 INVALID_BALANCE_TYPE

* Balance Type is invalid.

> -4045 MAX_STOP_ORDER_EXCEEDED

* Reach max stop order limit.

> -4046 NO_NEED_TO_CHANGE_MARGIN_TYPE

* No need to change margin type.

> -4047 THERE_EXISTS_OPEN_ORDERS

* Margin type cannot be changed if there exists open orders.

> -4048 THERE_EXISTS_QUANTITY

* Margin type cannot be changed if there exists position.

> -4049 ADD_ISOLATED_MARGIN_REJECT

* Add margin only support for isolated position.

> -4050 CROSS_BALANCE_INSUFFICIENT

* Cross balance insufficient.

> -4051 ISOLATED_BALANCE_INSUFFICIENT

* Isolated balance insufficient.

> -4052 NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN

* No need to change auto add margin.

> -4053 AUTO_ADD_CROSSED_MARGIN_REJECT

* Auto add margin only support for isolated position.

> -4054 ADD_ISOLATED_MARGIN_NO_POSITION_REJECT

* Cannot add position margin: position is 0.

> -4055 AMOUNT_MUST_BE_POSITIVE

* Amount must be positive.

> -4056 INVALID_API_KEY_TYPE

* Invalid api key type.

> -4057 INVALID_RSA_PUBLIC_KEY

* Invalid api public key

> -4058 MAX_PRICE_TOO_LARGE

* maxPrice and priceDecimal too large,please check.

> -4059 NO_NEED_TO_CHANGE_POSITION_SIDE

* No need to change position side.

> -4060 INVALID_POSITION_SIDE

* Invalid position side.

> -4061 POSITION_SIDE_NOT_MATCH

* Order's position side does not match user's setting.

> -4062 REDUCE_ONLY_CONFLICT

* Invalid or improper reduceOnly value.

> -4063 INVALID_OPTIONS_REQUEST_TYPE

* Invalid options request type

> -4064 INVALID_OPTIONS_TIME_FRAME

* Invalid options time frame

> -4065 INVALID_OPTIONS_AMOUNT

* Invalid options amount

> -4066 INVALID_OPTIONS_EVENT_TYPE

* Invalid options event type

> -4067 POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS

* Position side cannot be changed if there exists open orders.

> -4068 POSITION_SIDE_CHANGE_EXISTS_QUANTITY

* Position side cannot be changed if there exists position.

> -4069 INVALID_OPTIONS_PREMIUM_FEE

* Invalid options premium fee

> -4070 INVALID_CL_OPTIONS_ID_LEN

* Client options id is not valid.
* Client options id length should be less than 32 chars

> -4071 INVALID_OPTIONS_DIRECTION

* Invalid options direction

> -4072 OPTIONS_PREMIUM_NOT_UPDATE

* premium fee is not updated, reject order

> -4073 OPTIONS_PREMIUM_INPUT_LESS_THAN_ZERO

* input premium fee is less than 0, reject order

> -4074 OPTIONS_AMOUNT_BIGGER_THAN_UPPER

* Order amount is bigger than upper boundary or less than 0, reject order

> -4075 OPTIONS_PREMIUM_OUTPUT_ZERO

* output premium fee is less than 0, reject order

> -4076 OPTIONS_PREMIUM_TOO_DIFF

* original fee is too much higher than last fee

> -4077 OPTIONS_PREMIUM_REACH_LIMIT

* place order amount has reached to limit, reject order

> -4078 OPTIONS_COMMON_ERROR

* options internal error

> -4079 INVALID_OPTIONS_ID

* invalid options id
* invalid options id: %s
* duplicate options id %d for user %d

> -4080 OPTIONS_USER_NOT_FOUND

* user not found
* user not found with id: %s

> -4081 OPTIONS_NOT_FOUND

* options not found
* options not found with id: %s

> -4082 INVALID_BATCH_PLACE_ORDER_SIZE

* Invalid number of batch place orders.
* Invalid number of batch place orders: %s

> -4083 PLACE_BATCH_ORDERS_FAIL

* Fail to place batch orders.

> -4084 UPCOMING_METHOD

* Method is not allowed currently. Upcoming soon.

> -4085 INVALID_NOTIONAL_LIMIT_COEF

* Invalid notional limit coefficient

> -4086 INVALID_PRICE_SPREAD_THRESHOLD

* Invalid price spread threshold

> -4087 REDUCE_ONLY_ORDER_PERMISSION

* User can only place reduce only order

> -4088 NO_PLACE_ORDER_PERMISSION

* User can not place order currently

> -4104 INVALID_CONTRACT_TYPE

* Invalid contract type

> -4114 INVALID_CLIENT_TRAN_ID_LEN

* clientTranId  is not valid
* Client tran id length should be less than 64 chars

> -4115 DUPLICATED_CLIENT_TRAN_ID

* clientTranId  is duplicated
* Client tran id should be unique within 7 days

> -4118 REDUCE_ONLY_MARGIN_CHECK_FAILED

* ReduceOnly Order Failed. Please check your existing position and open orders

> -4131 MARKET_ORDER_REJECT

* The counterparty's best price does not meet the PERCENT_PRICE filter limit

> -4135 INVALID_ACTIVATION_PRICE

* Invalid activation price

> -4137 QUANTITY_EXISTS_WITH_CLOSE_POSITION

* Quantity must be zero with closePosition equals true

> -4138 REDUCE_ONLY_MUST_BE_TRUE

* Reduce only must be true with closePosition equals true

> -4139 ORDER_TYPE_CANNOT_BE_MKT

* Order type can not be market if it's unable to cancel

> -4140 INVALID_OPENING_POSITION_STATUS

* Invalid symbol status for opening position

> -4141 SYMBOL_ALREADY_CLOSED

* Symbol is closed

> -4142 STRATEGY_INVALID_TRIGGER_PRICE

* REJECT: take profit or stop order will be triggered immediately

> -4144 INVALID_PAIR

* Invalid pair

> -4161 ISOLATED_LEVERAGE_REJECT_WITH_POSITION

* Leverage reduction is not supported in Isolated Margin Mode with open positions

> -4164 MIN_NOTIONAL

* Order's notional must be no smaller than 5.0 (unless you choose reduce only)
* Order's notional must be no smaller than %s (unless you choose reduce only)

> -4165 INVALID_TIME_INTERVAL

* Invalid time interval
* Maximum time interval is %s days

> -4183 PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP

* Price is higher than stop price multiplier cap.
* Limit price can't be higher than %s.

> -4184 PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN

* Price is lower than stop price multiplier floor.
* Limit price can't be lower than %s.


---

# aster-finance-futures-api-v3_CN.md

- [基本信息](#基本信息)
	- [Rest 基本信息](#rest-基本信息)
		- [HTTP 返回代码](#http-返回代码)
		- [接口错误代码](#接口错误代码)
		- [接口的基本信息](#接口的基本信息)
	- [访问限制](#访问限制)
		- [IP 访问限制](#ip-访问限制)
		- [下单频率限制](#下单频率限制)
	- [接口鉴权类型](#接口鉴权类型)
	- [鉴权签名体](#鉴权签名体)
	- [需要签名的接口](#需要签名的接口)
	- [时间同步安全](#时间同步安全)
	- [POST /fapi/v3/order 的示例](#post-fapiv3order-的示例)
	- [GET /fapi/v3/order 的示例](#get-fapiv3order-的示例)
	- [完整python脚本示例](#完整python脚本示例)
	- [公开API参数](#公开api参数)
		- [术语解释](#术语解释)
		- [枚举定义](#枚举定义)
	- [过滤器](#过滤器)
		- [交易对过滤器](#交易对过滤器)
			- [PRICE_FILTER 价格过滤器](#price_filter-价格过滤器)
			- [LOT_SIZE 订单尺寸](#lot_size-订单尺寸)
			- [MARKET_LOT_SIZE 市价订单尺寸](#market_lot_size-市价订单尺寸)
			- [MAX_NUM_ORDERS 最多订单数](#max_num_orders-最多订单数)
			- [MAX_NUM_ALGO_ORDERS 最多条件订单数](#max_num_algo_orders-最多条件订单数)
			- [PERCENT_PRICE 价格振幅过滤器](#percent_price-价格振幅过滤器)
			- [MIN_NOTIONAL 最小名义价值](#min_notional-最小名义价值)
- [行情接口](#行情接口)
	- [测试服务器连通性 PING](#测试服务器连通性-ping)
	- [获取服务器时间](#获取服务器时间)
	- [获取交易规则和交易对](#获取交易规则和交易对)
	- [深度信息](#深度信息)
	- [近期成交](#近期成交)
	- [查询历史成交(MARKET_DATA)](#查询历史成交market_data)
	- [近期成交(归集)](#近期成交归集)
	- [K线数据](#k线数据)
	- [价格指数K线数据](#价格指数k线数据)
	- [标记价格K线数据](#标记价格k线数据)
	- [最新标记价格和资金费率](#最新标记价格和资金费率)
	- [查询资金费率历史](#查询资金费率历史)
	- [24hr价格变动情况](#24hr价格变动情况)
	- [最新价格](#最新价格)
	- [当前最优挂单](#当前最优挂单)
- [Websocket 行情推送](#websocket-行情推送)
	- [实时订阅/取消数据流](#实时订阅取消数据流)
		- [订阅一个信息流](#订阅一个信息流)
		- [取消订阅一个信息流](#取消订阅一个信息流)
		- [已订阅信息流](#已订阅信息流)
		- [设定属性](#设定属性)
		- [检索属性](#检索属性)
		- [错误信息](#错误信息)
	- [最新合约价格](#最新合约价格)
	- [归集交易](#归集交易)
	- [最新标记价格](#最新标记价格)
	- [全市场最新标记价格](#全市场最新标记价格)
	- [K线](#k线)
	- [按Symbol的精简Ticker](#按symbol的精简ticker)
	- [全市场的精简Ticker](#全市场的精简ticker)
	- [按Symbol的完整Ticker](#按symbol的完整ticker)
	- [全市场的完整Ticker](#全市场的完整ticker)
	- [按Symbol的最优挂单信息](#按symbol的最优挂单信息)
	- [全市场最优挂单信息](#全市场最优挂单信息)
	- [有限档深度信息](#有限档深度信息)
	- [增量深度信息](#增量深度信息)
	- [如何正确在本地维护一个orderbook副本](#如何正确在本地维护一个orderbook副本)
- [账户和交易接口](#账户和交易接口)
	- [更改持仓模式(TRADE)](#更改持仓模式trade)
	- [查询持仓模式(USER_DATA)](#查询持仓模式user_data)
	- [更改联合保证金模式(TRADE)](#更改联合保证金模式trade)
	- [查询联合保证金模式(USER_DATA)](#查询联合保证金模式user_data)
	- [下单 (TRADE)](#下单-trade)
	- [测试下单接口 (TRADE)](#测试下单接口-trade)
	- [批量下单 (TRADE)](#批量下单-trade)
	- [查询订单 (USER_DATA)](#查询订单-user_data)
	- [撤销订单 (TRADE)](#撤销订单-trade)
	- [撤销全部订单 (TRADE)](#撤销全部订单-trade)
	- [批量撤销订单 (TRADE)](#批量撤销订单-trade)
	- [倒计时撤销所有订单 (TRADE)](#倒计时撤销所有订单-trade)
	- [查询当前挂单 (USER_DATA)](#查询当前挂单-user_data)
	- [查看当前全部挂单 (USER_DATA)](#查看当前全部挂单-user_data)
	- [查询所有订单(包括历史订单) (USER_DATA)](#查询所有订单包括历史订单-user_data)
	- [账户余额v3 (USER_DATA)](#账户余额v3-user_data)
	- [账户信息v3 (USER_DATA)](#账户信息v3-user_data)
	- [调整开仓杠杆 (TRADE)](#调整开仓杠杆-trade)
	- [变换逐全仓模式 (TRADE)](#变换逐全仓模式-trade)
	- [调整逐仓保证金 (TRADE)](#调整逐仓保证金-trade)
	- [逐仓保证金变动历史 (TRADE)](#逐仓保证金变动历史-trade)
	- [用户持仓风险v3 (USER_DATA)](#用户持仓风险v3-user_data)
	- [账户成交历史 (USER_DATA)](#账户成交历史-user_data)
	- [获取账户损益资金流水(USER_DATA)](#获取账户损益资金流水user_data)
	- [杠杆分层标准 (USER_DATA)](#杠杆分层标准-user_data)
	- [持仓ADL队列估算 (USER_DATA)](#持仓adl队列估算-user_data)
	- [用户强平单历史 (USER_DATA)](#用户强平单历史-user_data)
	- [用户手续费率 (USER_DATA)](#用户手续费率-user_data)
- [Websocket 账户信息推送](#websocket-账户信息推送)
	- [生成listenKey (USER_STREAM)](#生成listenkey-user_stream)
	- [延长listenKey有效期 (USER_STREAM)](#延长listenkey有效期-user_stream)
	- [关闭listenKey (USER_STREAM)](#关闭listenkey-user_stream)
	- [listenKey 过期推送](#listenkey-过期推送)
	- [追加保证金通知](#追加保证金通知)
	- [Balance和Position更新推送](#balance和position更新推送)
	- [订单/交易 更新推送](#订单交易-更新推送)
	- [杠杆倍数等账户配置 更新推送](#杠杆倍数等账户配置-更新推送)
- [错误代码](#错误代码)
	- [10xx - 常规服务器或网络问题](#10xx---常规服务器或网络问题)
	- [11xx - Request issues](#11xx---request-issues)
	- [20xx - Processing Issues](#20xx---processing-issues)
	- [40xx - Filters and other Issues](#40xx---filters-and-other-issues)

# 基本信息


## Rest 基本信息

* 接口可能需要用户的AGENT，如何创建AGENT请参考[这里](https://www.asterdex.com/)
* 本篇列出REST接口的baseurl **https://fapi.asterdex.com**
* 所有接口的响应都是JSON格式
* 响应中如有数组，数组元素以时间升序排列，越早的数据越提前。
* 所有时间、时间戳均为UNIX时间，单位为毫秒
* 所有数据类型采用JAVA的数据类型定义

### HTTP 返回代码
* HTTP `4XX` 错误码用于指示错误的请求内容、行为、格式。
* HTTP `403` 错误码表示违反WAF限制(Web应用程序防火墙)。
* HTTP `429` 错误码表示警告访问频次超限，即将被封IP
* HTTP `418` 表示收到429后继续访问，于是被封了。
* HTTP `5XX` 错误码用于指示Aster Finance服务侧的问题。    
* HTTP `503` 表示API服务端已经向业务核心提交了请求但未能获取响应，特别需要注意的是其不代表请求失败，而是未知。很可能已经得到了执行，也有可能执行失败，需要做进一步确认。


### 接口错误代码
* 每个接口都有可能抛出异常

> 异常响应格式如下：

```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

* 具体的错误码及其解释在[错误代码](#错误代码)

### 接口的基本信息
* `GET`方法的接口, 参数必须在`query string`中发送.
* `POST`, `PUT`, 和 `DELETE` 方法的接口, 在 `request body`中发送(content type `application/x-www-form-urlencoded`)
* 对参数的顺序不做要求。

## 访问限制
* 在 `/fapi/v3/exchangeInfo`接口中`rateLimits`数组里包含有REST接口(不限于本篇的REST接口)的访问限制。包括带权重的访问频次限制、下单速率限制。本篇`枚举定义`章节有限制类型的进一步说明。
* 违反上述任何一个访问限制都会收到HTTP 429，这是一个警告.

<aside class="notice">
请注意，若用户被认定利用频繁挂撤单且故意低效交易意图发起攻击行为，Aster Finance有权视具体情况进一步加强对其访问限制。
</aside>


### IP 访问限制
* 每个请求将包含一个`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)`的头，其中包含当前IP所有请求的已使用权重。
* 每个路由都有一个"权重"，该权重确定每个接口计数的请求数。较重的接口和对多个交易对进行操作的接口将具有较重的"权重"。
* 收到429时，您有责任作为API退回而不向其发送更多的请求。
* **如果屡次违反速率限制和/或在收到429后未能退回，将导致API的IP被禁(http状态418)。**
* 频繁违反限制，封禁时间会逐渐延长 ，**对于重复违反者，将会被封从2分钟到3天**。
* **访问限制是基于IP的，而不是AGENT**

<aside class="notice">
强烈建议您尽可能多地使用websocket消息获取相应数据,既可以保障消息的及时性，也可以减少请求带来的访问限制压力。
</aside>


### 下单频率限制
* 每个下单请求回报将包含一个`X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)`的头，其中包含当前账户已用的下单限制数量。
* 被拒绝或不成功的下单并不保证回报中包含以上头内容。
* **下单频率限制是基于每个账户计数的。**

**关于交易时效性** 
互联网状况并不100%可靠，不可完全依赖,因此你的程序本地到服务器的时延会有抖动.
这是我们设置`recvWindow`的目的所在，如果你从事高频交易，对交易时效性有较高的要求，可以灵活设置recvWindow以达到你的要求。

<aside class="notice">
不推荐使用5秒以上的recvWindow
</aside>

## 接口鉴权类型
* 每个接口都有自己的鉴权类型，鉴权类型决定了访问时应当进行何种鉴权
* 如果需要鉴权，应当在请求体中添加signer

鉴权类型 | 描述
------------ | ------------
NONE | 不需要鉴权的接口
TRADE | 需要有效的signer和签名
USER_DATA | 需要有效的signer和签名
USER_STREAM | 需要有效的signer和签名
MARKET_DATA | 需要有效的signer和签名

## 鉴权签名体
参数 | 描述
------------ | ------------
user | 主账户钱包地址
signer | API钱包地址
nonce | 当前时间戳,单位为微秒
signature | 签名

## 需要签名的接口 
* TRADE 与 USER_DATA,USER_STREAM,MARKET_DATA
* 接口参数转字符串后按照key值ASCII编码后生成的字符串 请注意所有参数取值请以字符串的方式进行签名
* 生成字符串后在与鉴权签名参数的user,signer,nonce使用web3的abi参数编码生成字节码
* 生成字节码后使用Keccak算法生成hash
* 使用派生地址的私钥用web3的ecdsa签名算法对该hash进行签名生成signature

### 时间同步安全
* 签名接口均需要传递`timestamp`参数,其值应当是请求发送时刻的unix时间戳(毫秒)
* 服务器收到请求时会判断请求中的时间戳,如果是5000毫秒之前发出的,则请求会被认为无效。这个时间窗口值可以通过发送可选参数`recvWindow`来自定义。

> 逻辑伪代码：
  
  ```javascript
  if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow) {
    // process request
  } else {
    // reject request
  }
  ```

## POST /fapi/v3/order 的示例 

#### 所有参数均通过from body请求(Python 3.9.6)

#### 示例 : 以下参数为api注册信息,user,signer,privateKey仅供示范(privateKey为signer的私钥)

Key | Value
------------ | ------------
user | 0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e
signer | 0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0
privateKey | 0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1

#### 示例 : nonce参数为当前系统微秒值,超过系统时间,或者落后系统时间超过5s为非法请求
```python
#python
nonce = math.trunc(time.time()*1000000)
print(nonce)
#1748310859508867
```
```java
//java
Instant now = Instant.now();
long microsecond = now.getEpochSecond() * 1000000 + now.getNano() / 1000;
```

#### 示例 : 以下参数为业务请求参数 

```python
    my_dict = {'symbol': 'SANDUSDT', 'positionSide': 'BOTH', 'type': 'LIMIT', 'side': 'BUY',
	         'timeInForce': 'GTC', 'quantity': "190", 'price': 0.28694}
```

#### 示例 : 所有参数通过 form body 发送(方法以python为例) 

> **第一步将所有业务参数转字符串后按照ascII排序生成字符串:**

```python
    #定义所有元素取值转换为字符串
    def _trim_dict(my_dict) :
    # 假设待删除的字典为d
     for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

    #移除空值元素
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    # my_dict['timestamp'] = 1749545309665
    #将元素转换为字符串
    _trim_dict(my_dict)
    #根据ASCII排序生成字符串并移除特殊字符
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    {"positionSide":"BOTH","price":"0.28694","quantity":"190","recvWindow":"50000","side":"BUY","symbol":"SANDUSDT","timeInForce":"GTC","timestamp":"1749545309665","type":"LIMIT"}
```

> **第二步将第一步生成的字符串与账户信息以及nonce进行abi编码生成hash字符串:**

```python
   from eth_abi import encode
   from web3 import Web3
   #使用WEB3 ABI对生成的字符串和user, signer, nonce进行编码
   encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
   print(encoded.hex())
   #000000000000000000000000000000000000000000000000000000000000008000000000000000000000000063dd5acc6b1aa0f563956c0e534dd30b6dcf7c4e00000000000000000000000021cf8ae13bb72632562c6fff438652ba1a151bb00000000000000000000000000000000000000000000000000006361457bcec8300000000000000000000000000000000000000000000000000000000000000af7b22706f736974696f6e53696465223a22424f5448222c227072696365223a22302e3238363934222c227175616e74697479223a22313930222c227265637657696e646f77223a223530303030222c2273696465223a22425559222c2273796d626f6c223a2253414e4455534454222c2274696d65496e466f726365223a22475443222c2274696d657374616d70223a2231373439353435333039363635222c2274797065223a224c494d4954227d0000000000000000000000000000000000
   #keccak hex
   keccak_hex =Web3.keccak(encoded).hex()
   print(keccak_hex)
   #9e0273fc91323f5cdbcb00c358be3dee2854afb2d3e4c68497364a2f27a377fc
```
> **第三步将第二步生成的hash用privateKey进行签名:**
```python
    from eth_account import Account
    from eth_abi import encode
    from web3 import Web3, EthereumTesterProvider
    from eth_account.messages import encode_defunct

    signable_msg = encode_defunct(hexstr=keccak_hex)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    signature =  '0x'+signed_message.signature.hex()
    print(signature)
    #0x0337dd720a21543b80ff861cd3c26646b75b3a6a4b5d45805d4c1d6ad6fc33e65f0722778dd97525466560c69fbddbe6874eb4ed6f5fa7e576e486d9b5da67f31b
```
> **第四步将所有参数以及第三步生成的signature组装成请求体:**

```python
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()
    url ='https://fapi.asterdex.com/fapi/v3/order'
    headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
    }
    res = requests.post(url,data=my_dict,headers=headers)
    print(url)
    #curl  -X POST 'https://fapi.asterdex.com/fapi/v3/order' -d 'symbol=SANDUSDT&positionSide=BOTH&type=LIMIT&side=BUY&timeInForce=GTC&quantity=190&price=0.28694&recvWindow=50000&timestamp=1749545309665&nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0&signature=0x0337dd720a21543b80ff861cd3c26646b75b3a6a4b5d45805d4c1d6ad6fc33e65f0722778dd97525466560c69fbddbe6874eb4ed6f5fa7e576e486d9b5da67f31b'   

```

## GET /fapi/v3/order 的示例 
#### 示例 : 所有参数通过 query string 发送(Python 3.9.6) 

#### 示例 : 以下参数为api注册信息,user,signer,privateKey仅供示范(privateKey为agent的私钥)

Key | Value
------------ | ------------
user | 0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e
signer | 0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0
privateKey | 0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1

#### 示例 : nonce参数为当前系统微秒值,超过系统时间50s,或者落后系统时间超过5s为非法请求
```python
#python
nonce = math.trunc(time.time()*1000000)
print(nonce)
#1748310859508867
```
```java
//java
Instant now = Instant.now();
long microsecond = now.getEpochSecond() * 1000000 + now.getNano() / 1000;
```

#### 示例 : 以下参数为业务请求参数 
```python
my_dict = {'symbol':'SANDUSDT','side':"SELL","type":'LIMIT','orderId':2194215}
```

> **第一步将所有业务参数转字符串后按照ascII排序生成字符串:**

```python
    #定义所有元素取值转换为字符串
    def _trim_dict(my_dict) :
      for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

    #移除空值元素
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    # my_dict['timestamp'] = 1749545309665
    #将元素转换为字符串
    _trim_dict(my_dict)
    #根据ASCII排序生成字符串并移除特殊字符
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    #{"orderId":"2194215","recvWindow":"50000","side":"BUY","symbol":"SANDUSDT","timestamp":"1749545309665","type":"LIMIT"}
```

> **第二步将第一步生成的字符串与账户信息以及nonce进行abi编码生成hash字符串:**

```python
   from eth_abi import encode
   from web3 import Web3

   #使用WEB3 ABI对生成的字符串和user, signer, nonce进行编码
   encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
   print(encoded.hex())
   #000000000000000000000000000000000000000000000000000000000000008000000000000000000000000063dd5acc6b1aa0f563956c0e534dd30b6dcf7c4e00000000000000000000000021cf8ae13bb72632562c6fff438652ba1a151bb00000000000000000000000000000000000000000000000000006361457bcec8300000000000000000000000000000000000000000000000000000000000000767b226f726465724964223a2232313934323135222c227265637657696e646f77223a223530303030222c2273696465223a22425559222c2273796d626f6c223a2253414e4455534454222c2274696d657374616d70223a2231373439353435333039363635222c2274797065223a224c494d4954227d00000000000000000000
   keccak_hex =Web3.keccak(encoded).hex()
   print(keccak_hex)
   #6ad9569ea1355bf62de1b09b33b267a9404239af6d9227fa59e3633edae19e2a
```
> **第三步将第二步生成的hash用privateKey进行签名:**
```python
    from eth_account import Account
    from eth_abi import encode
    from web3 import Web3, EthereumTesterProvider
    from eth_account.messages import encode_defunct

    signable_msg = encode_defunct(hexstr=keccak_hex)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    signature =  '0x'+signed_message.signature.hex()
    print(signature)
    #0x4f5e36e91f0d4cf5b29b6559ebc2c808d3c808ebb13b2bcaaa478b98fb4195642c7473f0d1aa101359aaf278126af1a53bcb482fb05003bfb6bdc03de03c63151b
```
> **第四步将所有参数以及第三步生成的signature组装成请求体:**

```python
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()

    url ='https://fapi.asterdex.com/fapi/v3/order'

    res = requests.get(url, params=my_dict)
    print(url)
    #curl  -X GET 'https://fapi.asterdex.com/fapi/v3/order?symbol=SANDUSDT&side=BUY&type=LIMIT&orderId=2194215&recvWindow=50000&timestamp=1749545309665&nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0&signature=0x4f5e36e91f0d4cf5b29b6559ebc2c808d3c808ebb13b2bcaaa478b98fb4195642c7473f0d1aa101359aaf278126af1a53bcb482fb05003bfb6bdc03de03c63151b'

```
## 完整python脚本示例
```python
#Python 3.9.6
#eth-account~=0.13.7
#eth-abi~=5.2.0
#web3~=7.11.0
#requests~=2.32.3

import json
import math
import time
import requests

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

user = '0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e'
signer='0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0'
priKey = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
host = 'https://fapi.asterdex.com'
placeOrder = {'url': '/fapi/v3/order', 'method': 'POST',
              'params':{'symbol': 'SANDUSDT', 'positionSide': 'BOTH', 'type': 'LIMIT', 'side': 'BUY',
	         'timeInForce': 'GTC', 'quantity': "30", 'price': 0.325,'reduceOnly': True}}
getOrder = {'url':'/fapi/v3/order','method':'GET','params':{'symbol':'SANDUSDT','side':"BUY","type":'LIMIT','orderId':2194215}}

def call(api):
    nonce = math.trunc(time.time() * 1000000)
    my_dict = api['params']
    send(api['url'],api['method'],sign(my_dict,nonce))

def sign(my_dict,nonce):
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    msg = trim_param(my_dict,nonce)
    signable_msg = encode_defunct(hexstr=msg)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()

    print(my_dict['signature'])
    return  my_dict

def trim_param(my_dict,nonce) -> str:
    _trim_dict(my_dict)
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    print(json_str)
    encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
    print(encoded.hex())
    keccak_hex =Web3.keccak(encoded).hex()
    print(keccak_hex)
    return keccak_hex

def _trim_dict(my_dict) :
    for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

def send(url, method, my_dict):
    url = host + url
    print(url)
    print(my_dict)
    if method == 'POST':
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }
        res = requests.post(url, data=my_dict, headers=headers)
        print(res.text)
    if method == 'GET':
        res = requests.get(url, params=my_dict)
        print(res.text)
    if method == 'DELETE':
        res = requests.delete(url, data=my_dict)
        print(res.text)

if __name__ == '__main__':
    call(placeOrder)
    # call(getOrder)

```

## 公开API参数
### 术语解释
* `base asset` 指一个交易对的交易对象，即写在靠前部分的资产名
* `quote asset` 指一个交易对的定价资产，即写在靠后部分资产名


### 枚举定义

**交易对类型:**

* FUTURE 期货

**合约类型 (contractType):**

* PERPETUAL 永续合约


**合约状态 (contractStatus, status):**

* PENDING_TRADING   待上市
* TRADING          	交易中
* PRE_SETTLE			预结算
* SETTLING			结算中
* CLOSE				已下架


**订单状态 (status):**

* NEW 新建订单
* PARTIALLY_FILLED  部分成交
* FILLED  全部成交
* CANCELED  已撤销
* REJECTED 订单被拒绝
* EXPIRED 订单过期(根据timeInForce参数规则)

**订单种类 (orderTypes, type):**

* LIMIT 限价单
* MARKET 市价单
* STOP 止损限价单
* STOP_MARKET 止损市价单
* TAKE_PROFIT 止盈限价单
* TAKE_PROFIT_MARKET 止盈市价单
* TRAILING_STOP_MARKET 跟踪止损单

**订单方向 (side):**

* BUY 买入
* SELL 卖出

**持仓方向:**

* BOTH 单一持仓方向
* LONG 多头(双向持仓下)
* SHORT 空头(双向持仓下)

**有效方式 (timeInForce):**

* GTC - Good Till Cancel 成交为止
* IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
* FOK - Fill or Kill 无法全部立即成交就撤销
* GTX - Good Till Crossing 无法成为挂单方就撤销

**条件价格触发类型 (workingType)**

* MARK_PRICE
* CONTRACT_PRICE 

**响应类型 (newOrderRespType)**

* ACK
* RESULT

**K线间隔:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**限制种类 (rateLimitType)**

> REQUEST_WEIGHT

```javascript
  {
  	"rateLimitType": "REQUEST_WEIGHT",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 2400
  }
```

> ORDERS

```javascript
  {
  	"rateLimitType": "ORDERS",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 1200
   }
```

* REQUESTS_WEIGHT  单位时间请求权重之和上限

* ORDERS    单位时间下单(撤单)次数上限


**限制间隔**

* MINUTE



## 过滤器
过滤器，即Filter，定义了一系列交易规则。
共有两类，分别是针对交易对的过滤器`symbol filters`，和针对整个交易所的过滤器`exchange filters`(暂不支持)

### 交易对过滤器
#### PRICE_FILTER 价格过滤器

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "PRICE_FILTER",
    "minPrice": "0.00000100",
    "maxPrice": "100000.00000000",
    "tickSize": "0.00000100"
  }
```

价格过滤器用于检测order订单中price参数的合法性

* `minPrice` 定义了 `price`/`stopPrice` 允许的最小值
* `maxPrice` 定义了 `price`/`stopPrice` 允许的最大值。
* `tickSize` 定义了 `price`/`stopPrice` 的步进间隔，即price必须等于minPrice+(tickSize的整数倍)
以上每一项均可为0，为0时代表这一项不再做限制。

逻辑伪代码如下：

* `price` >= `minPrice`
* `price` <= `maxPrice`
* (`price`-`minPrice`) % `tickSize` == 0



#### LOT_SIZE 订单尺寸

> */exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

lots是拍卖术语，这个过滤器对订单中的`quantity`也就是数量参数进行合法性检查。包含三个部分：

* `minQty` 表示 `quantity` 允许的最小值.
* `maxQty` 表示 `quantity` 允许的最大值
* `stepSize` 表示 `quantity`允许的步进值。

逻辑伪代码如下：

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0


#### MARKET_LOT_SIZE 市价订单尺寸
参考LOT_SIZE，区别仅在于对市价单还是限价单生效

#### MAX_NUM_ORDERS 最多订单数


> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "MAX_NUM_ORDERS",
    "limit": 200
  }
```

定义了某个交易对最多允许的挂单数量(不包括已关闭的订单)

普通订单与条件订单均计算在内


#### MAX_NUM_ALGO_ORDERS 最多条件订单数

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MAX_NUM_ALGO_ORDERS",
    "limit": 100
  }
```

定义了某个交易对最多允许的条件订单的挂单数量(不包括已关闭的订单)。   

条件订单目前包括`STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`, 和 `TRAILING_STOP_MARKET`


#### PERCENT_PRICE 价格振幅过滤器

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "PERCENT_PRICE",
    "multiplierUp": "1.1500",
    "multiplierDown": "0.8500",
    "multiplierDecimal": 4
  }
```

`PERCENT_PRICE` 定义了基于标记价格计算的挂单价格的可接受区间.

挂单价格必须同时满足以下条件：

* 买单: `price` <= `markPrice` * `multiplierUp`
* 卖单: `price` >= `markPrice` * `multiplierDown`


#### MIN_NOTIONAL 最小名义价值

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "MIN_NOTIONAL",
    "notioanl": "1"
  }
```

MIN_NOTIONAL过滤器定义了交易对订单所允许的最小名义价值(成交额)。
订单的名义价值是`价格`*`数量`。 
由于`MARKET`订单没有价格，因此会使用 mark price 计算。   






---


# 行情接口
## 测试服务器连通性 PING
``
GET /fapi/v3/ping
``

> **响应:**

```javascript
{}
```

测试能否联通

**权重:**
1

**参数:**
NONE



## 获取服务器时间

> **响应:**

```javascript
{
  "serverTime": 1499827319559 // 当前的系统时间
}
```

``
GET /fapi/v3/time
``

获取服务器时间

**权重:**
1

**参数:**
NONE


## 获取交易规则和交易对

> **响应:**

```javascript
{
	"exchangeFilters": [],
 	"rateLimits": [ // API访问的限制
 		{
 			"interval": "MINUTE", // 按照分钟计算
   			"intervalNum": 1, // 按照1分钟计算
   			"limit": 2400, // 上限次数
   			"rateLimitType": "REQUEST_WEIGHT" // 按照访问权重来计算
   		},
  		{
  			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 1200,
   			"rateLimitType": "ORDERS" // 按照订单数量来计算
   		}
   	],
 	"serverTime": 1565613908500, // 请忽略。如果需要获取当前系统时间，请查询接口 “GET /fapi/v3/time”
 	"assets": [ // 资产信息
 		{
 			"asset": "BUSD",
   			"marginAvailable": true, // 是否可用作保证金
   			"autoAssetExchange": 0 // 保证金资产自动兑换阈值
   		},
 		{
 			"asset": "USDT",
   			"marginAvailable": true, // 是否可用作保证金
   			"autoAssetExchange": 0 // 保证金资产自动兑换阈值
   		},
 		{
 			"asset": "BNB",
   			"marginAvailable": false, // 是否可用作保证金
   			"autoAssetExchange": null // 保证金资产自动兑换阈值
   		}
   	],
 	"symbols": [ // 交易对信息
 		{
 			"symbol": "BLZUSDT",  // 交易对
 			"pair": "BLZUSDT",  // 标的交易对
 			"contractType": "PERPETUAL",	// 合约类型
 			"deliveryDate": 4133404800000,  // 交割日期
 			"onboardDate": 1598252400000,	  // 上线日期
 			"status": "TRADING",  // 交易对状态
 			"maintMarginPercent": "2.5000",  // 请忽略
 			"requiredMarginPercent": "5.0000", // 请忽略
 			"baseAsset": "BLZ",  // 标的资产
 			"quoteAsset": "USDT", // 报价资产
 			"marginAsset": "USDT", // 保证金资产
 			"pricePrecision": 5,  // 价格小数点位数(仅作为系统精度使用，注意同tickSize 区分）
 			"quantityPrecision": 0,  // 数量小数点位数(仅作为系统精度使用，注意同stepSize 区分）
 			"baseAssetPrecision": 8,  // 标的资产精度
 			"quotePrecision": 8,  // 报价资产精度
 			"underlyingType": "COIN",
 			"underlyingSubType": ["STORAGE"],
 			"settlePlan": 0,
 			"triggerProtect": "0.15", // 开启"priceProtect"的条件订单的触发阈值
 			"filters": [
 				{
 					"filterType": "PRICE_FILTER", // 价格限制
     				"maxPrice": "300", // 价格上限, 最大价格
     				"minPrice": "0.0001", // 价格下限, 最小价格
     				"tickSize": "0.0001" // 订单最小价格间隔
     			},
    			{
    				"filterType": "LOT_SIZE", // 数量限制
     				"maxQty": "10000000", // 数量上限, 最大数量
     				"minQty": "1", // 数量下限, 最小数量
     				"stepSize": "1" // 订单最小数量间隔
     			},
    			{
    				"filterType": "MARKET_LOT_SIZE", // 市价订单数量限制
     				"maxQty": "590119", // 数量上限, 最大数量
     				"minQty": "1", // 数量下限, 最小数量
     				"stepSize": "1" // 允许的步进值
     			},
     			{
    				"filterType": "MAX_NUM_ORDERS", // 最多订单数限制
    				"limit": 200
  				},
  				{
    				"filterType": "MAX_NUM_ALGO_ORDERS", // 最多条件订单数限制
    				"limit": 100
  				},
  				{
  					"filterType": "MIN_NOTIONAL",  // 最小名义价值
  					"notional": "1", 
  				},
  				{
    				"filterType": "PERCENT_PRICE", // 价格比限制
    				"multiplierUp": "1.1500", // 价格上限百分比
    				"multiplierDown": "0.8500", // 价格下限百分比
    				"multiplierDecimal": 4
    			}
   			],
 			"OrderType": [ // 订单类型
   				"LIMIT",  // 限价单
   				"MARKET",  // 市价单
   				"STOP", // 止损单
   				"STOP_MARKET", // 止损市价单
   				"TAKE_PROFIT", // 止盈单
   				"TAKE_PROFIT_MARKET", // 止盈暑市价单
   				"TRAILING_STOP_MARKET" // 跟踪止损市价单
   			],
   			"timeInForce": [ // 有效方式
   				"GTC", // 成交为止, 一直有效
   				"IOC", // 无法立即成交(吃单)的部分就撤销
   				"FOK", // 无法全部立即成交就撤销
   				"GTX" // 无法成为挂单方就撤销
 			],
 			"liquidationFee": "0.010000",	// 强平费率
   			"marketTakeBound": "0.30",	// 市价吃单(相对于标记价格)允许可造成的最大价格偏离比例
 		}
   	],
	"timezone": "UTC" // 服务器所用的时间区域
}

```

``
GET /fapi/v3/exchangeInfo
``

获取交易规则和交易对

**权重:**
1

**参数:**
NONE



## 深度信息

> **响应:**

```javascript
{
  "lastUpdateId": 1027024,
  "E": 1589436922972,   // 消息时间
  "T": 1589436922959,   // 撮合引擎时间
  "bids": [				// 买单
    [
      "4.00000000",     // 价格
      "431.00000000"    // 数量
    ]
  ],
  "asks": [				// 卖单
    [
      "4.00000200",		// 价格
      "12.00000000"		// 数量
    ]
  ]
}
```

``
GET /fapi/v3/depth
``

**权重:**

limit         | 权重
------------  | ------------
5, 10, 20, 50 | 2
100           | 5
500           | 10
1000          | 20

**参数:**

 名称  |  类型  | 是否必需 |                            描述
------ | ------ | -------- | -----------------------------------------------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认 500; 可选值:[5, 10, 20, 50, 100, 500, 1000]



## 近期成交

> **响应:**

```javascript
[
  {
    "id": 28457,				// 成交ID
    "price": "4.00000100",		// 成交价格
    "qty": "12.00000000",		// 成交量
    "quoteQty": "48.00",		// 成交额
    "time": 1499865549590,		// 时间
    "isBuyerMaker": true		// 买方是否为挂单方
  }
]
```

``
GET /fapi/v3/trades
``

获取近期订单簿成交

**权重:**
1

**参数:**

 名称  |  类型  | 是否必需 |          描述
------ | ------ | -------- | ----------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认:500，最大1000 

* 仅返回订单簿成交，即不会返回保险基金和自动减仓(ADL)成交

## 查询历史成交(MARKET_DATA)

> **响应:**

```javascript
[
  {
    "id": 28457,				// 成交ID
    "price": "4.00000100",		// 成交价格
    "qty": "12.00000000",		// 成交量
    "quoteQty": "48.00",		// 成交额
    "time": 1499865549590,		// 时间
    "isBuyerMaker": true		// 买方是否为挂单方
  }
]
```

``
GET /fapi/v3/historicalTrades
``

查询订单簿历史成交

**权重:**
20

**参数:**

 名称  |  类型  | 是否必需 |                      描述
------ | ------ | -------- | ----------------------------------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认值:500 最大值:1000.
fromId | LONG   | NO       | 从哪一条成交id开始返回. 缺省返回最近的成交记录

* 仅返回订单簿成交，即不会返回保险基金和自动减仓(ADL)成交

## 近期成交(归集)

> **响应:**

```javascript
[
  {
    "a": 26129,         // 归集成交ID
    "p": "0.01633102",  // 成交价
    "q": "4.70443515",  // 成交量
    "f": 27781,         // 被归集的首个成交ID
    "l": 27781,         // 被归集的末个成交ID
    "T": 1498793709153, // 成交时间
    "m": true,          // 是否为主动卖出单
  }
]
```

``
GET /fapi/v3/aggTrades
``

归集交易与逐笔交易的区别在于，同一价格、同一方向、同一时间(按秒计算)的订单簿trade会被聚合为一条

**权重:**
20

**参数:**

  名称    |  类型  | 是否必需 |                描述
--------- | ------ | -------- | ----------------------------------
symbol    | STRING | YES      | 交易对
fromId    | LONG   | NO       | 从包含fromID的成交开始返回结果
startTime | LONG   | NO       | 从该时刻之后的成交记录开始返回结果
endTime   | LONG   | NO       | 返回该时刻为止的成交记录
limit     | INT    | NO       | 默认 500; 最大 1000.

* 如果同时发送`startTime`和`endTime`，间隔必须小于一小时
* 如果没有发送任何筛选参数(`fromId`, `startTime`, `endTime`)，默认返回最近的成交记录
* 保险基金和自动减仓(ADL)成交不属于订单簿成交，故不会被归并聚合


## K线数据

> **响应:**

```javascript
[
  [
    1499040000000,      // 开盘时间
    "0.01634790",       // 开盘价
    "0.80000000",       // 最高价
    "0.01575800",       // 最低价
    "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
    "148976.11427815",  // 成交量
    1499644799999,      // 收盘时间
    "2434.19055334",    // 成交额
    308,                // 成交笔数
    "1756.87402397",    // 主动买入成交量
    "28.46694368",      // 主动买入成交额
    "17928899.62484339" // 请忽略该参数
  ]
]
```

``
GET /fapi/v3/klines
``

每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
symbol    | STRING | YES      | 交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500.

* 缺省返回最近的数据



## 价格指数K线数据

> **响应:**

```javascript
[
  [
    1591256400000,      	// 开盘时间
    "9653.69440000",    	// 开盘价
    "9653.69640000",     	// 最高价
    "9651.38600000",     	// 最低价
    "9651.55200000",     	// 收盘价(当前K线未结束的即为最新价)
    "0	", 					// 请忽略
    1591256459999,      	// 收盘时间
    "0",    				// 请忽略
    60,                		// 构成记录数
    "0",    				// 请忽略
    "0",      				// 请忽略
    "0" 					// 请忽略
  ]
]
```

``
GET /fapi/v3/indexPriceKlines
``

每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
pair    	| STRING | YES      | 标的交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500

* 缺省返回最近的数据


## 标记价格K线数据

> **响应:**

```javascript
[
  [
    1591256400000,      	// 开盘时间
    "9653.69440000",    	// 开盘价
    "9653.69640000",     	// 最高价
    "9651.38600000",     	// 最低价
    "9651.55200000",     	// 收盘价(当前K线未结束的即为最新价)
    "0	", 					// 请忽略
    1591256459999,      	// 收盘时间
    "0",    				// 请忽略
    60,                		// 构成记录数
    "0",    				// 请忽略
    "0",      				// 请忽略
    "0" 					// 请忽略
  ]
]
```

``
GET /fapi/v3/markPriceKlines
``
每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
symbol   	| STRING | YES      | 交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500

* 缺省返回最近的数据


## 最新标记价格和资金费率 

> **响应:**

```javascript
{
    "symbol": "BTCUSDT",				// 交易对
    "markPrice": "11793.63104562",		// 标记价格
    "indexPrice": "11781.80495970",		// 指数价格
    "estimatedSettlePrice": "11781.16138815",  // 预估结算价,仅在交割开始前最后一小时有意义
    "lastFundingRate": "0.00038246",	// 最近更新的资金费率
    "nextFundingTime": 1597392000000,	// 下次资金费时间
    "interestRate": "0.00010000",		// 标的资产基础利率
    "time": 1597370495002				// 更新时间
}
```

> **当不指定symbol时相应**

```javascript
[
	{
    	"symbol": "BTCUSDT",			// 交易对
    	"markPrice": "11793.63104562",	// 标记价格
    	"indexPrice": "11781.80495970",	// 指数价格
    	"estimatedSettlePrice": "11781.16138815",  // 预估结算价,仅在交割开始前最后一小时有意义
    	"lastFundingRate": "0.00038246",	// 最近更新的资金费率
    	"nextFundingTime": 1597392000000,	// 下次资金费时间
    	"interestRate": "0.00010000",		// 标的资产基础利率
    	"time": 1597370495002				// 更新时间
	}
]
```


``
GET /fapi/v3/premiumIndex
``

采集各大交易所数据加权平均

**权重:**
1

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对


## 查询资金费率历史

> **响应:**

```javascript
[
	{
    	"symbol": "BTCUSDT",			// 交易对
    	"fundingRate": "-0.03750000",	// 资金费率
    	"fundingTime": 1570608000000,	// 资金费时间
	},
	{
   		"symbol": "BTCUSDT",
    	"fundingRate": "0.00010000",
    	"fundingTime": 1570636800000,
	}
]
```

``
GET /fapi/v3/fundingRate
``

**权重:**
1

**参数:**

  名称    |  类型  | 是否必需 |                         描述
--------- | ------ | -------- | -----------------------------------------------------
symbol    | STRING | NO      | 交易对
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:100 最大值:1000

* 如果 `startTime` 和 `endTime` 都未发送, 返回最近 `limit` 条数据.
* 如果 `startTime` 和 `endTime` 之间的数据量大于 `limit`, 返回 `startTime` + `limit`情况下的数据。


## 24hr价格变动情况

> **响应:**

```javascript
{
  "symbol": "BTCUSDT",
  "priceChange": "-94.99999800",    //24小时价格变动
  "priceChangePercent": "-95.960",  //24小时价格变动百分比
  "weightedAvgPrice": "0.29628482", //加权平均价
  "lastPrice": "4.00000200",        //最近一次成交价
  "lastQty": "200.00000000",        //最近一次成交额
  "openPrice": "99.00000000",       //24小时内第一次成交的价格
  "highPrice": "100.00000000",      //24小时最高价
  "lowPrice": "0.10000000",         //24小时最低价
  "volume": "8913.30000000",        //24小时成交量
  "quoteVolume": "15.30000000",     //24小时成交额
  "openTime": 1499783499040,        //24小时内，第一笔交易的发生时间
  "closeTime": 1499869899040,       //24小时内，最后一笔交易的发生时间
  "firstId": 28385,   // 首笔成交id
  "lastId": 28460,    // 末笔成交id
  "count": 76         // 成交笔数
}
```

> 或(当不发送交易对信息)

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"priceChange": "-94.99999800",    //24小时价格变动
  		"priceChangePercent": "-95.960",  //24小时价格变动百分比
  		"weightedAvgPrice": "0.29628482", //加权平均价
  		"lastPrice": "4.00000200",        //最近一次成交价
  		"lastQty": "200.00000000",        //最近一次成交额
  		"openPrice": "99.00000000",       //24小时内第一次成交的价格
  		"highPrice": "100.00000000",      //24小时最高价
  		"lowPrice": "0.10000000",         //24小时最低价
  		"volume": "8913.30000000",        //24小时成交量
  		"quoteVolume": "15.30000000",     //24小时成交额
  		"openTime": 1499783499040,        //24小时内，第一笔交易的发生时间
  		"closeTime": 1499869899040,       //24小时内，最后一笔交易的发生时间
  		"firstId": 28385,   // 首笔成交id
  		"lastId": 28460,    // 末笔成交id
  		"count": 76         // 成交笔数
    }
]
```

``
GET /fapi/v3/ticker/24hr
``

请注意，不携带symbol参数会返回全部交易对数据，不仅数据庞大，而且权重极高

**权重:**
* 带symbol为`1`
* 不带为`40`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息


## 最新价格

> **响应:**

```javascript
{
  "symbol": "LTCBTC",		// 交易对
  "price": "4.00000200",		// 价格
  "time": 1589437530011   // 撮合引擎时间
}
```

> 或(当不发送symbol)

```javascript
[
	{
  		"symbol": "BTCUSDT",	// 交易对
  		"price": "6000.01",		// 价格
  		"time": 1589437530011   // 撮合引擎时间
	}
]
```

``
GET /fapi/v3/ticker/price
``

返回最近价格

**权重:**
* 单交易对`1`
* 无交易对`2`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息


## 当前最优挂单

> **响应:**

```javascript
{
  "symbol": "BTCUSDT", // 交易对
  "bidPrice": "4.00000000", //最优买单价
  "bidQty": "431.00000000", //挂单量
  "askPrice": "4.00000200", //最优卖单价
  "askQty": "9.00000000", //挂单量
  "time": 1589437530011   // 撮合引擎时间
}
```
> 或(当不发送symbol)

```javascript
[
	{
  		"symbol": "BTCUSDT", // 交易对
  		"bidPrice": "4.00000000", //最优买单价
  		"bidQty": "431.00000000", //挂单量
  		"askPrice": "4.00000200", //最优卖单价
  		"askQty": "9.00000000", //挂单量
  		"time": 1589437530011   // 撮合引擎时间
	}
]
```

``
GET /fapi/v3/ticker/bookTicker
``

返回当前最优的挂单(最高买单，最低卖单)

**权重:**
* 单交易对`1`   
* 无交易对`2`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息




# Websocket 行情推送

* 本篇所列出的所有wss接口baseurl: **wss://fstream.asterdex.com**
* 订阅单一stream格式为 **/ws/\<streamName\>**
* 组合streams的URL格式为 **/stream?streams=\<streamName1\>/\<streamName2\>/\<streamName3\>**
* 订阅组合streams时，事件payload会以这样的格式封装 **{"stream":"\<streamName\>","data":\<rawPayload\>}**
* stream名称中所有交易对均为**小写**
* 每个链接有效期不超过24小时，请妥善处理断线重连。
* 服务端每5分钟会发送ping帧，客户端应当在15分钟内回复pong帧，否则服务端会主动断开链接。允许客户端发送不成对的pong帧(即客户端可以以高于15分钟每次的频率发送pong帧保持链接)。
* Websocket服务器每秒最多接受10个订阅消息。
* 如果用户发送的消息超过限制，连接会被断开连接。反复被断开连接的IP有可能被服务器屏蔽。
* 单个连接最多可以订阅 **200** 个Streams。




## 实时订阅/取消数据流

* 以下数据可以通过websocket发送以实现订阅或取消订阅数据流。示例如下。
* 响应内容中的`id`是无符号整数，作为往来信息的唯一标识。

### 订阅一个信息流

> **响应**

  ```javascript
  {
    "result": null,
    "id": 1
  }
  ```

* **请求**

  	{    
    	"method": "SUBSCRIBE",    
    	"params":     
    	[   
      	"btcusdt@aggTrade",    
      	"btcusdt@depth"     
    	],    
    	"id": 1   
  	}



### 取消订阅一个信息流

> **响应**
  
  ```javascript
  {
    "result": null,
    "id": 312
  }
  ```

* **请求**

  {   
    "method": "UNSUBSCRIBE",    
    "params":     
    [    
      "btcusdt@depth"   
    ],    
    "id": 312   
  }



### 已订阅信息流

> **响应**
  
  ```javascript
  {
    "result": [
      "btcusdt@aggTrade"
    ],
    "id": 3
  }
  ```


* **请求**

  {   
    "method": "LIST_SUBSCRIPTIONS",    
    "id": 3   
  }     
 


### 设定属性
当前，唯一可以设置的属性是设置是否启用`combined`("组合")信息流。   
当使用`/ws/`("原始信息流")进行连接时，combined属性设置为`false`，而使用 `/stream/`进行连接时则将属性设置为`true`。


> **响应**
  
  ```javascript
  {
    "result": null
    "id": 5
  }
  ```

* **请求**

  {    
    "method": "SET_PROPERTY",    
    "params":     
    [   
      "combined",    
      true   
    ],    
    "id": 5   
  }




### 检索属性

> **响应**

  ```javascript
  {
    "result": true, // Indicates that combined is set to true.
    "id": 2
  }
  ```
  
* **请求**
  
  {   
    "method": "GET_PROPERTY",    
    "params":     
    [   
      "combined"   
    ],    
    "id": 2   
  }   
 



### 错误信息

错误信息 | 描述
---|---
{"code": 0, "msg": "Unknown property"} |  `SET_PROPERTY` 或 `GET_PROPERTY`中应用的参数无效
{"code": 1, "msg": "Invalid value type: expected Boolean"} | 仅接受`true`或`false`
{"code": 2, "msg": "Invalid request: property name must be a string"}| 提供的属性名无效
{"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"}| 参数`id`未提供或`id`值是无效类型
{"code": 2, "msg": "Invalid request: unknown variant %s, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28"} | 错字提醒，或提供的值不是预期类型
{"code": 2, "msg": "Invalid request: too many parameters"}| 数据中提供了不必要参数
{"code": 2, "msg": "Invalid request: property name must be a string"} | 未提供属性名
{"code": 2, "msg": "Invalid request: missing field `method` at line 1 column 73"} | 数据未提供`method`
{"code":3,"msg":"Invalid JSON: expected value at line %s column %s"} | JSON 语法有误.




## 最新合约价格
aggTrade中的价格'p'或ticker/miniTicker中的价格'c'均可以作为最新成交价。

## 归集交易

> **Payload:**

```javascript
{
  "e": "aggTrade",  // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBUSDT",    // 交易对
  "a": 5933014,		// 归集成交 ID
  "p": "0.001",     // 成交价格
  "q": "100",       // 成交量
  "f": 100,         // 被归集的首个交易ID
  "l": 105,         // 被归集的末次交易ID
  "T": 123456785,   // 成交时间
  "m": true         // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
}
```

同一价格、同一方向、同一时间(100ms计算)的trade会被聚合为一条.

**Stream Name:**       
``<symbol>@aggTrade``

**Update Speed:** 100ms





## 最新标记价格

> **Payload:**

```javascript
  {
    "e": "markPriceUpdate",  	// 事件类型
    "E": 1562305380000,      	// 事件时间
    "s": "BTCUSDT",          	// 交易对
    "p": "11794.15000000",   	// 标记价格
    "i": "11784.62659091",		// 现货指数价格
    "P": "11784.25641265",		// 预估结算价,仅在结算前最后一小时有参考价值
    "r": "0.00038167",       	// 资金费率
    "T": 1562306400000       	// 下次资金时间
  }
```


**Stream Name:**    
``<symbol>@markPrice`` 或 ``<symbol>@markPrice@1s``

**Update Speed:** 3000ms 或 1000ms






## 全市场最新标记价格

> **Payload:**

```javascript
[
  {
    "e": "markPriceUpdate",  	// 事件类型
    "E": 1562305380000,      	// 事件时间
    "s": "BTCUSDT",          	// 交易对
    "p": "11185.87786614",   	// 标记价格
    "i": "11784.62659091"		// 现货指数价格
    "P": "11784.25641265",		// 预估结算价,仅在结算前最后一小时有参考价值
    "r": "0.00030000",       	// 资金费率
    "T": 1562306400000       	// 下个资金时间
  }
]
```


**Stream Name:**    
``!markPrice@arr`` 或 ``!markPrice@arr@1s``

**Update Speed:** 3000ms 或 1000ms





## K线

> **Payload:**

```javascript
{
  "e": "kline",     // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBUSDT",    // 交易对
  "k": {
    "t": 123400000, // 这根K线的起始时间
    "T": 123460000, // 这根K线的结束时间
    "s": "BNBUSDT",  // 交易对
    "i": "1m",      // K线间隔
    "f": 100,       // 这根K线期间第一笔成交ID
    "L": 200,       // 这根K线期间末一笔成交ID
    "o": "0.0010",  // 这根K线期间第一笔成交价
    "c": "0.0020",  // 这根K线期间末一笔成交价
    "h": "0.0025",  // 这根K线期间最高成交价
    "l": "0.0015",  // 这根K线期间最低成交价
    "v": "1000",    // 这根K线期间成交量
    "n": 100,       // 这根K线期间成交笔数
    "x": false,     // 这根K线是否完结(是否已经开始下一根K线)
    "q": "1.0000",  // 这根K线期间成交额
    "V": "500",     // 主动买入的成交量
    "Q": "0.500",   // 主动买入的成交额
    "B": "123456"   // 忽略此参数
  }
}
```

K线stream逐秒推送所请求的K线种类(最新一根K线)的更新。推送间隔250毫秒(如有刷新)

**订阅Kline需要提供间隔参数，最短为分钟线，最长为月线。支持以下间隔:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Stream Name:**    
``<symbol>@kline_<interval>``

**Update Speed:** 250ms




## 按Symbol的精简Ticker

> **Payload:**

```javascript
  {
    "e": "24hrMiniTicker",  // 事件类型
    "E": 123456789,         // 事件时间(毫秒)
    "s": "BNBUSDT",          // 交易对
    "c": "0.0025",          // 最新成交价格
    "o": "0.0010",          // 24小时前开始第一笔成交价格
    "h": "0.0025",          // 24小时内最高成交价
    "l": "0.0010",          // 24小时内最低成交价
    "v": "10000",           // 成交量
    "q": "18"               // 成交额
  }
```

按Symbol刷新的24小时精简ticker信息.

**Stream Name:**     
``<symbol>@miniTicker`

**Update Speed:** 500ms



## 全市场的精简Ticker

> **Payload:**

```javascript
[  
  {
    "e": "24hrMiniTicker",  // 事件类型
    "E": 123456789,         // 事件时间(毫秒)
    "s": "BNBUSDT",          // 交易对
    "c": "0.0025",          // 最新成交价格
    "o": "0.0010",          // 24小时前开始第一笔成交价格
    "h": "0.0025",          // 24小时内最高成交价
    "l": "0.0010",          // 24小时内最低成交价
    "v": "10000",           // 成交量
    "q": "18"               // 成交额
  }
]
```

所有symbol24小时精简ticker信息.需要注意的是，只有发生变化的ticker更新才会被推送。

**Stream Name:**     
`!miniTicker@arr`

**Update Speed:** 1000ms




## 按Symbol的完整Ticker


> **Payload:**

```javascript
{
  "e": "24hrTicker",  // 事件类型
  "E": 123456789,     // 事件时间
  "s": "BNBUSDT",      // 交易对
  "p": "0.0015",      // 24小时价格变化
  "P": "250.00",      // 24小时价格变化(百分比)
  "w": "0.0018",      // 平均价格
  "c": "0.0025",      // 最新成交价格
  "Q": "10",          // 最新成交价格上的成交量
  "o": "0.0010",      // 24小时内第一比成交的价格
  "h": "0.0025",      // 24小时内最高成交价
  "l": "0.0010",      // 24小时内最低成交价
  "v": "10000",       // 24小时内成交量
  "q": "18",          // 24小时内成交额
  "O": 0,             // 统计开始时间
  "C": 86400000,      // 统计关闭时间
  "F": 0,             // 24小时内第一笔成交交易ID
  "L": 18150,         // 24小时内最后一笔成交交易ID
  "n": 18151          // 24小时内成交数
}
```

按Symbol刷新的24小时完整ticker信息

**Stream Name:**     
``<symbol>@ticker``

**Update Speed:** 500ms



## 全市场的完整Ticker


> **Payload:**

```javascript
[
	{
	  "e": "24hrTicker",  // 事件类型
	  "E": 123456789,     // 事件时间
	  "s": "BNBUSDT",      // 交易对
	  "p": "0.0015",      // 24小时价格变化
	  "P": "250.00",      // 24小时价格变化(百分比)
	  "w": "0.0018",      // 平均价格
	  "c": "0.0025",      // 最新成交价格
	  "Q": "10",          // 最新成交价格上的成交量
	  "o": "0.0010",      // 24小时内第一比成交的价格
	  "h": "0.0025",      // 24小时内最高成交价
	  "l": "0.0010",      // 24小时内最低成交价
	  "v": "10000",       // 24小时内成交量
	  "q": "18",          // 24小时内成交额
	  "O": 0,             // 统计开始时间
	  "C": 86400000,      // 统计结束时间
	  "F": 0,             // 24小时内第一笔成交交易ID
	  "L": 18150,         // 24小时内最后一笔成交交易ID
	  "n": 18151          // 24小时内成交数
	}
]	
```

所有symbol 24小时完整ticker信息.需要注意的是，只有发生变化的ticker更新才会被推送。

**Stream Name:**     
``!ticker@arr``

**Update Speed:** 1000ms


## 按Symbol的最优挂单信息

> **Payload:**

```javascript
{
  "e":"bookTicker",		// 事件类型
  "u":400900217,     	// 更新ID
  "E": 1568014460893,	// 事件推送时间
  "T": 1568014460891,	// 撮合时间
  "s":"BNBUSDT",     	// 交易对
  "b":"25.35190000", 	// 买单最优挂单价格
  "B":"31.21000000", 	// 买单最优挂单数量
  "a":"25.36520000", 	// 卖单最优挂单价格
  "A":"40.66000000"  	// 卖单最优挂单数量
}
```


实时推送指定交易对最优挂单信息

**Stream Name:** `<symbol>@bookTicker`

**Update Speed:** 实时





## 全市场最优挂单信息

> **Payload:**

```javascript
{
  // Same as <symbol>@bookTicker payload
}
```

所有交易对交易对最优挂单信息

**Stream Name:** `!bookTicker`

**Update Speed:** 实时



##强平订单

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // 事件类型
	"E":1568014460893,                  // 事件时间
	"o":{
	
		"s":"BTCUSDT",                   // 交易对
		"S":"SELL",                      // 订单方向
		"o":"LIMIT",                     // 订单类型
		"f":"IOC",                       // 有效方式
		"q":"0.014",                     // 订单数量
		"p":"9910",                      // 订单价格
		"ap":"9910",                     // 平均价格
		"X":"FILLED",                    // 订单状态
		"l":"0.014",                     // 订单最近成交量
		"z":"0.014",                     // 订单累计成交量
		"T":1568014460893,          	 // 交易时间
	
	}

}
```

推送特定`symbol`的强平订单快照信息。

1000ms内至多仅推送一条最近的强平订单作为快照

**Stream Name:**  ``<symbol>@forceOrder``

**Update Speed:** 1000ms





## 有限档深度信息

> **Payload:**

```javascript
{
  "e": "depthUpdate", 			// 事件类型
  "E": 1571889248277, 			// 事件时间
  "T": 1571889248276, 			// 交易时间
  "s": "BTCUSDT",
  "U": 390497796,
  "u": 390497878,
  "pu": 390497794,
  "b": [          				// 买方
    [
      "7403.89",  				// 价格
      "0.002"     				// 数量
    ],
    [
      "7403.90",
      "3.906"
    ],
    [
      "7404.00",
      "1.428"
    ],
    [
      "7404.85",
      "5.239"
    ],
    [
      "7405.43",
      "2.562"
    ]
  ],
  "a": [          				// 卖方
    [
      "7405.96",  				// 价格
      "3.340"     				// 数量
    ],
    [
      "7406.63",
      "4.525"
    ],
    [
      "7407.08",
      "2.475"
    ],
    [
      "7407.15",
      "4.800"
    ],
    [
      "7407.20",
      "0.175"
    ]
  ]
}
```

推送有限档深度信息。levels表示几档买卖单信息, 可选 5/10/20档

**Stream Names:** `<symbol>@depth<levels>` 或 `<symbol>@depth<levels>@500ms` 或 `<symbol>@depth<levels>@100ms`.  

**Update Speed:** 250ms 或 500ms 或 100ms




## 增量深度信息

> **Payload:**

```javascript
{
  "e": "depthUpdate", 	// 事件类型
  "E": 123456789,     	// 事件时间
  "T": 123456788,     	// 撮合时间
  "s": "BNBUSDT",      	// 交易对
  "U": 157,           	// 从上次推送至今新增的第一个 update Id
  "u": 160,           	// 从上次推送至今新增的最后一个 update Id
  "pu": 149,          	// 上次推送的最后一个update Id(即上条消息的‘u’)
  "b": [              	// 变动的买单深度
    [
      "0.0024",       	// 价格
      "10"           	// 数量
    ]
  ],
  "a": [              	// 变动的卖单深度
    [
      "0.0026",       	// 价格
      "100"          	// 数量
    ]
  ]
}
```

orderbook的变化部分，推送间隔250毫秒,500毫秒，100毫秒(如有刷新)

**Stream 名称:**     
``<symbol>@depth`` OR ``<symbol>@depth@500ms`` OR ``<symbol>@depth@100ms``

**Update Speed:** 250ms 或 500ms 或 100ms


## 如何正确在本地维护一个orderbook副本
1. 订阅 **wss://fstream.asterdex.com/stream?streams=btcusdt@depth**
2. 开始缓存收到的更新。同一个价位，后收到的更新覆盖前面的。
3. 访问Rest接口 **https://fapi.asterdex.com/fapi/v3/depth?symbol=BTCUSDT&limit=1000**获得一个1000档的深度快照
4. 将目前缓存到的信息中`u`< 步骤3中获取到的快照中的`lastUpdateId`的部分丢弃(丢弃更早的信息，已经过期)。
5. 将深度快照中的内容更新到本地orderbook副本中，并从websocket接收到的第一个`U` <= `lastUpdateId` **且** `u` >= `lastUpdateId` 的event开始继续更新本地副本。
6. 每一个新event的`pu`应该等于上一个event的`u`，否则可能出现了丢包，请从step3重新进行初始化。
7. 每一个event中的挂单量代表这个价格目前的挂单量**绝对值**，而不是相对变化。
8. 如果某个价格对应的挂单量为0，表示该价位的挂单已经撤单或者被吃，应该移除这个价位。




# 账户和交易接口

<aside class="warning">
考虑到剧烈行情下, RESTful接口可能存在查询延迟，我们强烈建议您优先从Websocket user data stream推送的消息来获取订单，成交，仓位等信息。
</aside>


## 更改持仓模式(TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v3/positionSide/dual (HMAC SHA256)
``

变换用户在 ***所有symbol*** 合约上的持仓模式：双向持仓或单向持仓。   

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
dualSidePosition | STRING   | YES      | "true": 双向持仓模式；"false": 单向持仓模式
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 查询持仓模式(USER_DATA)

> **响应:**

```javascript
{
	"dualSidePosition": true // "true": 双向持仓模式；"false": 单向持仓模式
}
```

``
GET /fapi/v3/positionSide/dual (HMAC SHA256)
``

查询用户目前在 ***所有symbol*** 合约上的持仓模式：双向持仓或单向持仓。     

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 更改联合保证金模式(TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v3/multiAssetsMargin (HMAC SHA256)
``

变换用户在 ***所有symbol*** 合约上的联合保证金模式：开启或关闭联合保证金模式。   

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
multiAssetsMargin | STRING   | YES      | "true": 联合保证金模式开启；"false": 联合保证金模式关闭
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 查询联合保证金模式(USER_DATA)

> **响应:**

```javascript
{
	"multiAssetsMargin": true // "true": 联合保证金模式开启；"false": 联合保证金模式关闭
}
```

``
GET /fapi/v3/multiAssetsMargin (HMAC SHA256)
``

查询用户目前在 ***所有symbol*** 合约上的联合保证金模式。      

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 下单 (TRADE)


> **响应:**

```javascript
{
 	"clientOrderId": "testOrder", // 用户自定义的订单号
 	"cumQty": "0",
 	"cumQuote": "0", // 成交金额
 	"executedQty": "0", // 成交量
 	"orderId": 22542179, // 系统订单号
 	"avgPrice": "0.00000",	// 平均成交价
 	"origQty": "10", // 原始委托数量
 	"price": "0", // 委托价格
 	"reduceOnly": false, // 仅减仓
 	"side": "SELL", // 买卖方向
 	"positionSide": "SHORT", // 持仓方向
 	"status": "NEW", // 订单状态
 	"stopPrice": "0", // 触发价，对`TRAILING_STOP_MARKET`无效
 	"closePosition": false,   // 是否条件全平仓
 	"symbol": "BTCUSDT", // 交易对
 	"timeInForce": "GTC", // 有效方法
 	"type": "TRAILING_STOP_MARKET", // 订单类型
 	"origType": "TRAILING_STOP_MARKET",  // 触发前订单类型
 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
 	"updateTime": 1566818724722, // 更新时间
 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
POST /fapi/v3/order  (HMAC SHA256)
``

**权重:**
1

**参数:**

名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ---
symbol           | STRING  | YES      | 交易对
side             | ENUM    | YES      | 买卖方向 `SELL`, `BUY`
positionSide     | ENUM	    | NO       | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT`  
type             | ENUM    | YES      | 订单类型 `LIMIT`, `MARKET`, `STOP`, `TAKE_PROFIT`, `STOP_MARKET`, `TAKE_PROFIT_MARKET`, `TRAILING_STOP_MARKET`
reduceOnly       | STRING  | NO       | `true`, `false`; 非双开模式下默认`false`；双开模式下不接受此参数； 使用`closePosition`不支持此参数。
quantity         | DECIMAL | NO     	 | 下单数量,使用`closePosition`不支持此参数。
price            | DECIMAL | NO       | 委托价格
newClientOrderId | STRING  | NO       | 用户自定义的订单号，不可以重复出现在挂单中。如空缺系统会自动赋值。必须满足正则规则 `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice        | DECIMAL | NO       | 触发价, 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
closePosition    | STRING  | NO       | `true`, `false`；触发后全部平仓，仅支持`STOP_MARKET`和`TAKE_PROFIT_MARKET`；不与`quantity`合用；自带只平仓效果，不与`reduceOnly` 合用
activationPrice  | DECIMAL | NO       | 追踪止损激活价格，仅`TRAILING_STOP_MARKET` 需要此参数, 默认为下单当前市场价格(支持不同`workingType`)
callbackRate     | DECIMAL | NO       | 追踪止损回调比例，可取值范围[0.1, 5],其中 1代表1% ,仅`TRAILING_STOP_MARKET` 需要此参数
timeInForce      | ENUM    | NO       | 有效方法
workingType      | ENUM    | NO       | stopPrice 触发类型: `MARK_PRICE`(标记价格), `CONTRACT_PRICE`(合约最新价). 默认 `CONTRACT_PRICE`
priceProtect | STRING | NO | 条件单触发保护："TRUE","FALSE", 默认"FALSE". 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", 默认 "ACK"
recvWindow       | LONG    | NO       |
timestamp        | LONG    | YES      |

根据 order `type`的不同，某些参数强制要求，具体如下:

Type                 |           强制要求的参数
----------------------------------- | ----------------------------------
`LIMIT`                             | `timeInForce`, `quantity`, `price`
`MARKET`                            | `quantity`
`STOP`, `TAKE_PROFIT`               | `quantity`,  `price`, `stopPrice`
`STOP_MARKET`, `TAKE_PROFIT_MARKET` | `stopPrice`
`TRAILING_STOP_MARKET`              | `callbackRate`



* 条件单的触发必须:
	
	* 如果订单参数`priceProtect`为true:
		* 达到触发价时，`MARK_PRICE`(标记价格)与`CONTRACT_PRICE`(合约最新价)之间的价差不能超过改symbol触发保护阈值
		* 触发保护阈值请参考接口`GET /fapi/v3/exchangeInfo` 返回内容相应symbol中"triggerProtect"字段

	* `STOP`, `STOP_MARKET` 止损单:
		* 买入: 最新合约价格/标记价格高于等于触发价`stopPrice`
		* 卖出: 最新合约价格/标记价格低于等于触发价`stopPrice`
	* `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 止盈单:
		* 买入: 最新合约价格/标记价格低于等于触发价`stopPrice`
		* 卖出: 最新合约价格/标记价格高于等于触发价`stopPrice`

	* `TRAILING_STOP_MARKET` 跟踪止损单:
		* 买入: 当合约价格/标记价格区间最低价格低于激活价格`activationPrice`,且最新合约价格/标记价高于等于最低价设定回调幅度。
		* 卖出: 当合约价格/标记价格区间最高价格高于激活价格`activationPrice`,且最新合约价格/标记价低于等于最高价设定回调幅度。

* `TRAILING_STOP_MARKET` 跟踪止损单如果遇到报错 ``{"code": -2021, "msg": "Order would immediately trigger."}``    
表示订单不满足以下条件:
	* 买入: 指定的`activationPrice` 必须小于 latest price
	* 卖出: 指定的`activationPrice` 必须大于 latest price

* `newOrderRespType` 如果传 `RESULT`:
	* `MARKET` 订单将直接返回成交结果；
	* 配合使用特殊 `timeInForce` 的 `LIMIT` 订单将直接返回成交或过期拒绝结果。

* `STOP_MARKET`, `TAKE_PROFIT_MARKET` 配合 `closePosition`=`true`:
	* 条件单触发依照上述条件单触发逻辑
	* 条件触发后，平掉当时持有所有多头仓位(若为卖单)或当时持有所有空头仓位(若为买单)
	* 不支持 `quantity` 参数
	* 自带只平仓属性，不支持`reduceOnly`参数
	* 双开模式下,`LONG`方向上不支持`BUY`; `SHORT` 方向上不支持`SELL`


## 测试下单接口 (TRADE)


> **响应:**

```javascript
字段与下单接口一致，但均为无效值
```


``
POST /fapi/v3/order/test (HMAC SHA256)
``

用于测试订单请求，但不会提交到撮合引擎

**权重:**
1

**参数:**

参考 `POST /fapi/v3/order`



## 批量下单 (TRADE)


> **响应:**

```javascript
[
	{
	 	"clientOrderId": "testOrder", // 用户自定义的订单号
	 	"cumQty": "0",
	 	"cumQuote": "0", // 成交金额
	 	"executedQty": "0", // 成交量
	 	"orderId": 22542179, // 系统订单号
	 	"avgPrice": "0.00000",	// 平均成交价
	 	"origQty": "10", // 原始委托数量
	 	"price": "0", // 委托价格
	 	"reduceOnly": false, // 仅减仓
	 	"side": "SELL", // 买卖方向
	 	"positionSide": "SHORT", // 持仓方向
	 	"status": "NEW", // 订单状态
	 	"stopPrice": "0", // 触发价，对`TRAILING_STOP_MARKET`无效
	 	"closePosition": false,   // 是否条件全平仓
	 	"symbol": "BTCUSDT", // 交易对
	 	"timeInForce": "GTC", // 有效方法
	 	"type": "TRAILING_STOP_MARKET", // 订单类型
	 	"origType": "TRAILING_STOP_MARKET",  // 触发前订单类型
	 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	 	"updateTime": 1566818724722, // 更新时间
	 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
	 	"priceProtect": false            // 是否开启条件单触发保护
	},
	{
		"code": -2022, 
		"msg": "ReduceOnly Order is rejected."
	}
]
```

``
POST /fapi/v3/batchOrders  (HMAC SHA256)
``

**权重:**
5

**参数:**


名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
batchOrders |	list<JSON> | 	YES |	订单列表，最多支持5个订单
recvWindow |	LONG |	NO	
timestamp	| LONG | YES	

**其中``batchOrders``应以list of JSON格式填写订单参数**

名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
symbol           | STRING  | YES      | 交易对
side             | ENUM    | YES      | 买卖方向 `SELL`, `BUY`
positionSide     | ENUM	    | NO       | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT`   
type             | ENUM    | YES      | 订单类型 `LIMIT`, `MARKET`, `STOP`, `TAKE_PROFIT`, `STOP_MARKET`, `TAKE_PROFIT_MARKET`, `TRAILING_STOP_MARKET`
reduceOnly       | STRING  | NO       | `true`, `false`; 非双开模式下默认`false`；双开模式下不接受此参数。
quantity         | DECIMAL | YES      | 下单数量
price            | DECIMAL | NO       | 委托价格
newClientOrderId | STRING  | NO       | 用户自定义的订单号，不可以重复出现在挂单中。如空缺系统会自动赋值. 必须满足正则规则 `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice        | DECIMAL | NO       | 触发价, 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
activationPrice  | DECIMAL | NO       | 追踪止损激活价格，仅`TRAILING_STOP_MARKET` 需要此参数, 默认为下单当前市场价格(支持不同`workingType`)
callbackRate     | DECIMAL | NO       | 追踪止损回调比例，可取值范围[0.1, 4],其中 1代表1% ,仅`TRAILING_STOP_MARKET` 需要此参数
timeInForce      | ENUM    | NO       | 有效方法
workingType      | ENUM    | NO       | stopPrice 触发类型: `MARK_PRICE`(标记价格), `CONTRACT_PRICE`(合约最新价). 默认 `CONTRACT_PRICE`
priceProtect | STRING | NO | 条件单触发保护："TRUE","FALSE", 默认"FALSE". 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", 默认 "ACK"


* 具体订单条件规则，与普通下单一致
* 批量下单采取并发处理，不保证订单撮合顺序
* 批量下单的返回内容顺序，与订单列表顺序一致




## 查询订单 (USER_DATA)


> **响应:**

```javascript
{
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",					// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1573346959,				// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",						// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", 			// 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020",			// 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",					// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
GET /fapi/v3/order (HMAC SHA256)
``

查询订单状态

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单的最终状态为 `CANCELED` 或者 `EXPIRED`, **并且** 
	* 订单没有任何的成交记录, **并且**
	* 订单生成时间 + 7天 < 当前时间

**权重:**
1

**参数:**

名称        |  类型  | 是否必需 | 描述
----------------- | ------ | -------- | ----
symbol            | STRING | YES      | 交易对
orderId           | LONG   | NO       | 系统订单号
origClientOrderId | STRING | NO       | 用户自定义的订单号
recvWindow        | LONG   | NO       |
timestamp         | LONG   | YES      |

注意:

* 至少需要发送 `orderId` 与 `origClientOrderId`中的一个


## 撤销订单 (TRADE)

> **响应:**

```javascript
{
 	"clientOrderId": "myOrder1", // 用户自定义的订单号
 	"cumQty": "0",
 	"cumQuote": "0", // 成交金额
 	"executedQty": "0", // 成交量
 	"orderId": 283194212, // 系统订单号
 	"origQty": "11", // 原始委托数量
 	"price": "0", // 委托价格
	"reduceOnly": false, // 仅减仓
	"side": "BUY", // 买卖方向
	"positionSide": "SHORT", // 持仓方向
 	"status": "CANCELED", // 订单状态
 	"stopPrice": "9300", // 触发价，对`TRAILING_STOP_MARKET`无效
 	"closePosition": false,   // 是否条件全平仓
 	"symbol": "BTCUSDT", // 交易对
 	"timeInForce": "GTC", // 有效方法
 	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
 	"type": "TRAILING_STOP_MARKET", // 订单类型
 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
 	"updateTime": 1571110484038, // 更新时间
 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
DELETE /fapi/v3/order  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

名称               |  类型   | 是否必需  |        描述
----------------- | ------ | -------- | ------------------
symbol            | STRING | YES      | 交易对
orderId           | LONG   | NO       | 系统订单号
origClientOrderId | STRING | NO       | 用户自定义的订单号
recvWindow        | LONG   | NO       |
timestamp         | LONG   | YES      |

`orderId` 与 `origClientOrderId` 必须至少发送一个


## 撤销全部订单 (TRADE)

> **响应:**

```javascript
{
	"code": "200", 
	"msg": "The operation of cancel all open order is done."
}
```

``
DELETE /fapi/v3/allOpenOrders  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol     | STRING | YES      | 交易对
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 批量撤销订单 (TRADE)

> **响应:**

```javascript
[
	{
	 	"clientOrderId": "myOrder1", // 用户自定义的订单号
	 	"cumQty": "0",
	 	"cumQuote": "0", // 成交金额
	 	"executedQty": "0", // 成交量
	 	"orderId": 283194212, // 系统订单号
	 	"origQty": "11", // 原始委托数量
	 	"price": "0", // 委托价格
		"reduceOnly": false, // 仅减仓
		"side": "BUY", // 买卖方向
		"positionSide": "SHORT", // 持仓方向
	 	"status": "CANCELED", // 订单状态
	 	"stopPrice": "9300", // 触发价，对`TRAILING_STOP_MARKET`无效
	 	"closePosition": false,   // 是否条件全平仓
	 	"symbol": "BTCUSDT", // 交易对
	 	"timeInForce": "GTC", // 有效方法
	 	"origType": "TRAILING_STOP_MARKET", // 触发前订单类型
 		"type": "TRAILING_STOP_MARKET", // 订单类型
	 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  		"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	 	"updateTime": 1571110484038, // 更新时间
	 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
	 	"priceProtect": false            // 是否开启条件单触发保护
	},
	{
		"code": -2011,
		"msg": "Unknown order sent."
	}
]
```

``
DELETE /fapi/v3/batchOrders  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

  名称          |      类型      | 是否必需 |       描述
--------------------- | -------------- | -------- | ----------------
symbol                | STRING         | YES      | 交易对
orderIdList           | LIST\<LONG\>   | NO       | 系统订单号, 最多支持10个订单 <br/> 比如`[1234567,2345678]`
origClientOrderIdList | LIST\<STRING\> | NO       | 用户自定义的订单号, 最多支持10个订单 <br/> 比如`["my_id_1","my_id_2"]` 需要encode双引号。逗号后面没有空格。
recvWindow            | LONG           | NO       |
timestamp             | LONG           | YES      |

`orderIdList` 与 `origClientOrderIdList` 必须至少发送一个，不可同时发送


## 倒计时撤销所有订单 (TRADE)

> **响应:**

```javascript
{
	"symbol": "BTCUSDT", 
	"countdownTime": "100000"
}
```


``
POST /fapi/v3/countdownCancelAll  (HMAC SHA256)
``

**权重:**
10

**Parameters:**

  名称          |      类型      | 是否必需 |       描述
--------------------- | -------------- | -------- | ----------------
symbol | STRING | YES |
countdownTime | LONG | YES | 倒计时。 1000 表示 1 秒； 0 表示取消倒计时撤单功能。
recvWindow | LONG | NO |
timestamp | LONG | YES |

* 该接口可以被用于确保在倒计时结束时撤销指定symbol上的所有挂单。 在使用这个功能时，接口应像心跳一样在倒计时内被反复调用，以便可以取消既有的倒计时并开始新的倒数计时设置。

* 用法示例：
	以30s的间隔重复此接口，每次倒计时countdownTime设置为120000(120s)。   
	如果在120秒内未再次调用此接口，则您指定symbol上的所有挂单都会被自动撤销。   
	如果在120秒内以将countdownTime设置为0，则倒数计时器将终止，自动撤单功能取消。
	
* 系统会**大约每10毫秒**检查一次所有倒计时情况，因此请注意，使用此功能时应考虑足够的冗余。    
我们不建议将倒记时设置得太精确或太小。





## 查询当前挂单 (USER_DATA)

> **响应:**

```javascript

{
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"status": "NEW",					// 订单状态
  	"positionSide": "SHORT", // 持仓方向
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
GET /fapi/v3/openOrder  (HMAC SHA256)
``

请小心使用不带symbol参数的调用

**权重: 1**


**参数:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol | STRING | YES | 交易对
orderId | LONG | NO | 系统订单号
origClientOrderId | STRING | NO | 用户自定义的订单号
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* `orderId` 与 `origClientOrderId` 中的一个为必填参数
* 查询的订单如果已经成交或取消，将返回报错 "Order does not exist."


## 查看当前全部挂单 (USER_DATA)

> **响应:**

```javascript
[
  {
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", // 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
  }
]
```

``
GET /fapi/v3/openOrders  (HMAC SHA256)
``

请小心使用不带symbol参数的调用

**权重:**
- 带symbol ***1***
- 不带 ***40***

**参数:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol     | STRING | NO      | 交易对
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 不带symbol参数，会返回所有交易对的挂单



## 查询所有订单(包括历史订单) (USER_DATA)


> **响应:**

```javascript
[
  {
   	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", // 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,  			// 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
  }
]
```

``
GET /fapi/v3/allOrders (HMAC SHA256)
``

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单的最终状态为 `CANCELED` 或者 `EXPIRED`, **并且** 
	* 订单没有任何的成交记录, **并且**
	* 订单生成时间 + 7天 < 当前时间

**权重:**
5 

**Parameters:**

   名称    |  类型  | 是否必需 |                      描述
---------- | ------ | -------- | -----------------------------------------------
symbol     | STRING | YES      | 交易对
orderId    | LONG   | NO       | 只返回此orderID及之后的订单，缺省返回最近的订单
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值:500 最大值:1000
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 查询时间范围最大不得超过7天
* 默认查询最近7天内的数据



## 账户余额v3 (USER_DATA)

> **响应:**

```javascript
[
 	{
 		"accountAlias": "SgsR",    // 账户唯一识别码
 		"asset": "USDT",		// 资产
 		"balance": "122607.35137903",	// 总余额
 		"crossWalletBalance": "23.72469206", // 全仓余额
  		"crossUnPnl": "0.00000000"  // 全仓持仓未实现盈亏
  		"availableBalance": "23.72469206",       // 下单可用余额
  		"maxWithdrawAmount": "23.72469206",     // 最大可转出余额
  		"marginAvailable": true,    // 是否可用作联合保证金
  		"updateTime": 1617939110373
	}
]
```

``
GET /fapi/v3/balance (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES




## 账户信息v3 (USER_DATA)

> **响应:**

```javascript

{
	"feeTier": 0,  // 手续费等级
 	"canTrade": true,  // 是否可以交易
 	"canDeposit": true,  // 是否可以入金
 	"canWithdraw": true, // 是否可以出金
 	"updateTime": 0,
 	"totalInitialMargin": "0.00000000",  // 但前所需起始保证金总额(存在逐仓请忽略), 仅计算usdt资产
 	"totalMaintMargin": "0.00000000",  // 维持保证金总额, 仅计算usdt资产
 	"totalWalletBalance": "23.72469206",   // 账户总余额, 仅计算usdt资产
 	"totalUnrealizedProfit": "0.00000000",  // 持仓未实现盈亏总额, 仅计算usdt资产
 	"totalMarginBalance": "23.72469206",  // 保证金总余额, 仅计算usdt资产
 	"totalPositionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格), 仅计算usdt资产
 	"totalOpenOrderInitialMargin": "0.00000000",  // 当前挂单所需起始保证金(基于最新标记价格), 仅计算usdt资产
 	"totalCrossWalletBalance": "23.72469206",  // 全仓账户余额, 仅计算usdt资产
 	"totalCrossUnPnl": "0.00000000",	// 全仓持仓未实现盈亏总额, 仅计算usdt资产
 	"availableBalance": "23.72469206",       // 可用余额, 仅计算usdt资产
 	"maxWithdrawAmount": "23.72469206"     // 最大可转出余额, 仅计算usdt资产
 	"assets": [
 		{
 			"asset": "USDT",	 	//资产
 			"walletBalance": "23.72469206",  //余额
		   	"unrealizedProfit": "0.00000000",  // 未实现盈亏
		   	"marginBalance": "23.72469206",  // 保证金余额
		   	"maintMargin": "0.00000000",	// 维持保证金
		   	"initialMargin": "0.00000000",  // 当前所需起始保证金
		   	"positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
		   	"crossWalletBalance": "23.72469206",  //全仓账户余额
		   	"crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
		   	"availableBalance": "23.72469206",       // 可用余额
		   	"maxWithdrawAmount": "23.72469206",     // 最大可转出余额
		   	"marginAvailable": true,   // 是否可用作联合保证金
		   	"updateTime": 1625474304765  //更新时间
		},
		{
 			"asset": "BUSD",	 	//资产
 			"walletBalance": "103.12345678",  //余额
		   	"unrealizedProfit": "0.00000000",  // 未实现盈亏
		   	"marginBalance": "103.12345678",  // 保证金余额
		   	"maintMargin": "0.00000000",	// 维持保证金
		   	"initialMargin": "0.00000000",  // 当前所需起始保证金
		   	"positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
		   	"crossWalletBalance": "103.12345678",  //全仓账户余额
		   	"crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
		   	"availableBalance": "103.12345678",       // 可用余额
		   	"maxWithdrawAmount": "103.12345678",     // 最大可转出余额
		   	"marginAvailable": true,   // 否可用作联合保证金
		   	"updateTime": 0  // 更新时间
	       }
	],
 	"positions": [  // 头寸，将返回所有市场symbol。
 		//根据用户持仓模式展示持仓方向，即单向模式下只返回BOTH持仓情况，双向模式下只返回 LONG 和 SHORT 持仓情况
 		{
		 	"symbol": "BTCUSDT",  // 交易对
		   	"initialMargin": "0",	// 当前所需起始保证金(基于最新标记价格)
		   	"maintMargin": "0",	//维持保证金
		   	"unrealizedProfit": "0.00000000",  // 持仓未实现盈亏
		   	"positionInitialMargin": "0",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0",  // 当前挂单所需起始保证金(基于最新标记价格)
		   	"leverage": "100",	// 杠杆倍率
		   	"isolated": true,  // 是否是逐仓模式
		   	"entryPrice": "0.00000",  // 持仓成本价
		   	"maxNotional": "250000",  // 当前杠杆下用户可用的最大名义价值
		   	"positionSide": "BOTH",  // 持仓方向
		   	"positionAmt": "0",		 // 持仓数量
		   	"updateTime": 0         // 更新时间 
		}
  	]
}
```


``
GET /fapi/v3/account (HMAC SHA256)
``

**权重:**
5

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES |




## 调整开仓杠杆 (TRADE)

> **响应:**

```javascript
{
 	"leverage": 21,	// 杠杆倍数
 	"maxNotionalValue": "1000000", // 当前杠杆倍数下允许的最大名义价值
 	"symbol": "BTCUSDT"	// 交易对
}
```

``
POST /fapi/v3/leverage (HMAC SHA256)
``

调整用户在指定symbol合约的开仓杠杆。

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |            描述
---------- | ------ | -------- | ---------------------------
symbol     | STRING | YES      | 交易对
leverage   | INT    | YES      | 目标杠杆倍数：1 到 125 整数
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 变换逐全仓模式 (TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v3/marginType (HMAC SHA256)
``

变换用户在指定symbol合约上的保证金模式：逐仓或全仓。

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
symbol     | STRING | YES      | 交易对
marginType | ENUM   | YES      | 保证金模式 ISOLATED(逐仓), CROSSED(全仓)
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 调整逐仓保证金 (TRADE)

> **响应:**

```javascript
{
	"amount": 100.0,
  	"code": 200,
  	"msg": "Successfully modify position margin.",
  	"type": 1
}
```

``
POST /fapi/v3/positionMargin (HMAC SHA256)
``

针对逐仓模式下的仓位，调整其逐仓保证金资金。

**权重:**
1

**参数:**

   名称    |  类型   | 是否必需 |                 描述
---------- | ------- | -------- | ------------------------------------
symbol     | STRING  | YES      | 交易对
positionSide| ENUM   | NO		  | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT` 
amount     | DECIMAL | YES      | 保证金资金
type       | INT     | YES      | 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
recvWindow | LONG    | NO       |
timestamp  | LONG    | YES      |

* 只针对逐仓symbol 与 positionSide(如有)


## 逐仓保证金变动历史 (TRADE)

> **响应:**

```javascript
[
	{
		"amount": "23.36332311", // 数量
	  	"asset": "USDT", // 资产
	  	"symbol": "BTCUSDT", // 交易对
	  	"time": 1578047897183, // 时间
	  	"type": 1，	// 调整方向
	  	"positionSide": "BOTH"  // 持仓方向
	},
	{
		"amount": "100",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047900425,
	  	"type": 1，
	  	"positionSide": "LONG" 
	}
]
```

``
GET /fapi/v3/positionMargin/history (HMAC SHA256)
``



**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |                 描述
---------- | ------ | -------- | ------------------------------------
symbol     | STRING | YES      | 交易对
type       | INT    | NO       | 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值: 500
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |




## 用户持仓风险v3 (USER_DATA)

> **响应:**

> 单向持仓模式下：

```javascript
[
  	{
  		"entryPrice": "0.00000", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "0.00000000",	// 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "0", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "0.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "0.00000000", // 持仓未实现盈亏
  		"positionSide": "BOTH", // 持仓方向
  		"updateTime": 1625474304765   // 更新时间
  	}
]
```

> 双向持仓模式下：

```javascript
[
  	{
  		"entryPrice": "6563.66500", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "15517.54150468", // 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "5930.78", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "20.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "2316.83423560" // 持仓未实现盈亏
  		"positionSide": "LONG", // 持仓方向
  		"updateTime": 1625474304765  // 更新时间
  	},
  	{
  		"entryPrice": "0.00000", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "5413.95799991", // 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "7189.95", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "-10.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "-1156.46711780" // 持仓未实现盈亏
  		"positionSide": "SHORT", // 持仓方向
  		"updateTime": 1625474304765  //更新时间
  	}  	
]
```

``
GET /fapi/v3/positionRisk (HMAC SHA256)
``

**权重:**
5

**参数:**

   名称    | 类型 | 是否必需 | 描述
---------- | ---- | -------- | ----
symbol     | STRING | NO     |
recvWindow | LONG | NO       |
timestamp  | LONG | YES      |


**注意**    
请与账户推送信息`ACCOUNT_UPDATE`配合使用，以满足您的及时性和准确性需求。




## 账户成交历史 (USER_DATA)


> **响应:**

```javascript
[
  {
  	"buyer": false,	// 是否是买方
  	"commission": "-0.07819010", // 手续费
  	"commissionAsset": "USDT", // 手续费计价单位
  	"id": 698759,	// 交易ID
  	"maker": false,	// 是否是挂单方
  	"orderId": 25851813, // 订单编号
  	"price": "7819.01",	// 成交价
  	"qty": "0.002",	// 成交量
  	"quoteQty": "15.63802",	// 成交额
  	"realizedPnl": "-0.91539999",	// 实现盈亏
  	"side": "SELL",	// 买卖方向
  	"positionSide": "SHORT",  // 持仓方向
  	"symbol": "BTCUSDT", // 交易对
  	"time": 1569514978020 // 时间
  }
]
```

``
GET /fapi/v3/userTrades  (HMAC SHA256)
``

获取某交易对的成交历史

**权重:**
5

**参数:**

   名称    |  类型  | 是否必需 |                     描述
---------- | ------ | -------- | --------------------------------------------
symbol     | STRING | YES      | 交易对
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
fromId     | LONG   | NO       | 返回该fromId及之后的成交，缺省返回最近的成交
limit      | INT    | NO       | 返回的结果集数量 默认值:500 最大值:1000.
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 如果`startTime` 和 `endTime` 均未发送, 只会返回最近7天的数据。
* startTime 和 endTime 的最大间隔为7天


## 获取账户损益资金流水(USER_DATA)

> **响应:**

```javascript
[
	{
    	"symbol": "", // 交易对，仅针对涉及交易对的资金流
    	"incomeType": "TRANSFER",	// 资金流类型
    	"income": "-0.37500000", // 资金流数量，正数代表流入，负数代表流出
    	"asset": "USDT", // 资产内容
    	"info":"TRANSFER", // 备注信息，取决于流水类型
    	"time": 1570608000000, // 时间
    	"tranId":"9689322392",		// 划转ID
    	"tradeId":""					// 引起流水产生的原始交易ID
	},
	{
   		"symbol": "BTCUSDT",
    	"incomeType": "COMMISSION", 
    	"income": "-0.01000000",
    	"asset": "USDT",
    	"info":"COMMISSION",
    	"time": 1570636800000,
    	"tranId":"9689322392",		
    	"tradeId":"2059192"					
	}
]
```

``
GET /fapi/v3/income (HMAC SHA256)
``

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |                                              描述
---------- | ------ | -------- | -----------------------------------------------------------------------------------------------
symbol     | STRING | NO       | 交易对
incomeType | STRING | NO       | 收益类型 "TRANSFER"，"WELCOME_BONUS", "REALIZED_PNL"，"FUNDING_FEE", "COMMISSION", "INSURANCE_CLEAR", and "MARKET_MERCHANT_RETURN_REWARD"
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值:100 最大值:1000
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 如果`startTime` 和 `endTime` 均未发送, 只会返回最近7天的数据。
* 如果`incomeType`没有发送，返回所有类型账户损益资金流水。
* "trandId" 在相同用户的同一种收益流水类型中是唯一的。


## 杠杆分层标准 (USER_DATA)


> **响应:**

```javascript
[
    {
        "symbol": "ETHUSDT",
        "brackets": [
            {
                "bracket": 1,   // 层级
                "initialLeverage": 75,  // 该层允许的最高初始杠杆倍数
                "notionalCap": 10000,  // 该层对应的名义价值上限
                "notionalFloor": 0,  // 该层对应的名义价值下限 
                "maintMarginRatio": 0.0065, // 该层对应的维持保证金率
                "cum":0 // 速算数
            },
        ]
    }
]
```

> **或** (若发送symbol)

```javascript

{
    "symbol": "ETHUSDT",
    "brackets": [
        {
            "bracket": 1,
            "initialLeverage": 75,
            "notionalCap": 10000,
            "notionalFloor": 0,
            "maintMarginRatio": 0.0065,
            "cum":0
        },
    ]
}
```


``
GET /fapi/v3/leverageBracket
``


**权重:** 1

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol	| STRING | NO
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 持仓ADL队列估算 (USER_DATA)


> **响应:**

```javascript
[
	{
		"symbol": "ETHUSDT", 
		"adlQuantile": 
			{
				// 对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE", 其中"HEDGE"的存在仅作为标记;如果多空均有持仓的情况下,"LONG"和"SHORT"应返回共同计算后相同的队列分数。
				"LONG": 3,  
				"SHORT": 3, 
				"HEDGE": 0   // HEDGE 仅作为指示出现，请忽略数值
			}
		},
 	{
 		"symbol": "BTCUSDT", 
 		"adlQuantile": 
 			{
 				// 对于单向持仓模式或者是逐仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "BOTH" 分别表示不同持仓方向上持仓的adl队列分数
 				"LONG": 1, 	// 双开模式下多头持仓的ADL队列估算分
 				"SHORT": 2, 	// 双开模式下空头持仓的ADL队列估算分
 				"BOTH": 0		// 单开模式下持仓的ADL队列估算分
 			}
 	}
 ]
```

``
GET /fapi/v3/adlQuantile
``


**权重:** 5

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol	| STRING | NO
recvWindow|LONG|NO| 
timestamp|LONG|YES|

* 每30秒更新数据

* 队列分数0，1，2，3，4，分数越高说明在ADL队列中的位置越靠前

* 对于单向持仓模式或者是逐仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "BOTH" 分别表示不同持仓方向上持仓的adl队列分数

* 对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE", 其中"HEDGE"的存在仅作为标记;其中如果多空均有持仓的情况下,"LONG"和"SHORT"返回共同计算后相同的队列分数。


## 用户强平单历史 (USER_DATA)


> **响应:**

```javascript
[
  {
  	"orderId": 6071832819, 
  	"symbol": "BTCUSDT", 
  	"status": "FILLED", 
  	"clientOrderId": "autoclose-1596107620040000020", 
  	"price": "10871.09", 
  	"avgPrice": "10913.21000", 
  	"origQty": "0.001", 
  	"executedQty": "0.001", 
  	"cumQuote": "10.91321", 
  	"timeInForce": "IOC", 
  	"type": "LIMIT", 
  	"reduceOnly": false, 
  	"closePosition": false, 
  	"side": "SELL", 
  	"positionSide": "BOTH", 
  	"stopPrice": "0", 
  	"workingType": "CONTRACT_PRICE", 
  	"origType": "LIMIT", 
  	"time": 1596107620044, 
  	"updateTime": 1596107620087
  }
  {
   	"orderId": 6072734303, 
   	"symbol": "BTCUSDT", 
   	"status": "FILLED", 
   	"clientOrderId": "adl_autoclose", 
   	"price": "11023.14", 
   	"avgPrice": "10979.82000", 
   	"origQty": "0.001", 
   	"executedQty": "0.001", 
   	"cumQuote": "10.97982", 
   	"timeInForce": "GTC", 
   	"type": "LIMIT", 
   	"reduceOnly": false, 
   	"closePosition": false, 
   	"side": "BUY", 
   	"positionSide": "SHORT", 
   	"stopPrice": "0", 
   	"workingType": "CONTRACT_PRICE", 
   	"origType": "LIMIT", 
   	"time": 1596110725059, 
   	"updateTime": 1596110725071
  }
]
```


``
GET /fapi/v3/forceOrders
``


**权重:** 带symbol 20, 不带symbol 50

**参数:**

  名称      |  类型  | 是否必需 |                   描述
------------- | ------ | -------- | ----------------------------------------
symbol        | STRING | NO       |
autoCloseType | ENUM   | NO       | "LIQUIDATION": 强平单, "ADL": ADL减仓单.
startTime     | LONG   | NO       |
endTime       | LONG   | NO       |
limit         | INT    | NO       | Default 50; max 100.
recvWindow    | LONG   | NO       |
timestamp     | LONG   | YES      |

* 如果没有传 "autoCloseType", 强平单和ADL减仓单都会被返回
* 如果没有传"startTime", 只会返回"endTime"之前7天内的数据



## 用户手续费率 (USER_DATA)

> **响应:**

```javascript
{
	"symbol": "BTCUSDT",
  	"makerCommissionRate": "0.0002",  // 0.02%
  	"takerCommissionRate": "0.0004"   // 0.04%
}
```

``
GET /fapi/v3/commissionRate (HMAC SHA256)
``

**权重:**
20


**参数:**

名称  |  类型  | 是否必需 |  描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES	
recvWindow | LONG | NO	
timestamp | LONG | YES






# Websocket 账户信息推送


* 本篇所列出REST接口的baseurl **https://fapi.asterdex.com**
* 用于订阅账户数据的 `listenKey` 从创建时刻起有效期为60分钟
* 可以通过`PUT`一个`listenKey`延长60分钟有效期
* 可以通过`DELETE`一个 `listenKey` 立即关闭当前数据流，并使该`listenKey` 无效
* 在具有有效`listenKey`的帐户上执行`POST`将返回当前有效的`listenKey`并将其有效期延长60分钟
* 本篇所列出的websocket接口baseurl: **wss://fstream.asterdex.com**
* 订阅账户数据流的stream名称为 **/ws/\<listenKey\>**
* 每个链接有效期不超过24小时，请妥善处理断线重连。
* 账户数据流的消息**不保证**严格时间序; **请使用 E 字段进行排序**
* 考虑到剧烈行情下, RESTful接口可能存在查询延迟，我们强烈建议您优先从Websocket user data stream推送的消息来获取订单，仓位等信息。


## 生成listenKey (USER_STREAM)


> **响应:**

```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

``
POST /fapi/v3/listenKey
``

创建一个新的user data stream，返回值为一个listenKey，即websocket订阅的stream名称。如果该帐户具有有效的`listenKey`，则将返回该`listenKey`并将其有效期延长60分钟。

**权重:**
1

**参数:**

None


## 延长listenKey有效期 (USER_STREAM)


> **响应:**

```javascript
{}
```

``
PUT /fapi/v3/listenKey
``

有效期延长至本次调用后60分钟

**权重:**
1

**参数:**

None



## 关闭listenKey (USER_STREAM)

> **响应:**

```javascript
{}
```

``
DELETE /fapi/v3/listenKey
``

关闭某账户数据流

**权重:**
1

**参数:**

None



## listenKey 过期推送

> **Payload:**

```javascript
{
	'e': 'listenKeyExpired',      // 事件类型
	'E': 1576653824250				// 事件时间
}
```

当前连接使用的有效listenKey过期时，user data stream 将会推送此事件。

**注意:**

* 此事件与websocket连接中断没有必然联系
* 只有正在连接中的有效`listenKey`过期时才会收到此消息
* 收到此消息后user data stream将不再更新，直到用户使用新的有效的`listenKey`




## 追加保证金通知

> **Payload:**

```javascript
{
    "e":"MARGIN_CALL",    	// 事件类型
    "E":1587727187525,		// 事件时间
    "cw":"3.16812045",		// 除去逐仓仓位保证金的钱包余额, 仅在全仓 margin call 情况下推送此字段
    "p":[					// 涉及持仓
      {
        "s":"ETHUSDT",		// symbol
        "ps":"LONG",		// 持仓方向
        "pa":"1.327",		// 仓位
        "mt":"CROSSED",		// 保证金模式
        "iw":"0",			// 若为逐仓，仓位保证金
        "mp":"187.17127",	// 标记价格
        "up":"-1.166074",	// 未实现盈亏
        "mm":"1.614445"		// 持仓需要的维持保证金
      }
    ]
}  
 
```


* 当用户持仓风险过高，会推送此消息。
* 此消息仅作为风险指导信息，不建议用于投资策略。
* 在大波动市场行情下,不排除此消息发出的同时用户仓位已被强平的可能。




## Balance和Position更新推送

> **Payload:**

```javascript
{
  "e": "ACCOUNT_UPDATE",				// 事件类型
  "E": 1564745798939,            		// 事件时间
  "T": 1564745798938 ,           		// 撮合时间
  "a":                          		// 账户更新事件
    {
      "m":"ORDER",						// 事件推出原因 
      "B":[                     		// 余额信息
        {
          "a":"USDT",           		// 资产名称
          "wb":"122624.12345678",    	// 钱包余额
          "cw":"100.12345678",			// 除去逐仓仓位保证金的钱包余额
          "bc":"50.12345678"			// 除去盈亏与交易手续费以外的钱包余额改变量
        },
        {
          "a":"BUSD",           
          "wb":"1.00000000",
          "cw":"0.00000000",         
          "bc":"-49.12345678"
        }
      ],
      "P":[
       {
          "s":"BTCUSDT",          	// 交易对
          "pa":"0",               	// 仓位
          "ep":"0.00000",            // 入仓价格
          "cr":"200",             	// (费前)累计实现损益
          "up":"0",						// 持仓未实现盈亏
          "mt":"isolated",				// 保证金模式
          "iw":"0.00000000",			// 若为逐仓，仓位保证金
          "ps":"BOTH"					// 持仓方向
       }，
       {
        	"s":"BTCUSDT",
        	"pa":"20",
        	"ep":"6563.66500",
        	"cr":"0",
        	"up":"2850.21200",
        	"mt":"isolated",
        	"iw":"13200.70726908",
        	"ps":"LONG"
      	 },
       {
        	"s":"BTCUSDT",
        	"pa":"-10",
        	"ep":"6563.86000",
        	"cr":"-45.04000000",
        	"up":"-1423.15600",
        	"mt":"isolated",
        	"iw":"6570.42511771",
        	"ps":"SHORT"
       }
      ]
    }
}
```

账户更新事件的 event type 固定为 `ACCOUNT_UPDATE`

* 当账户信息有变动时，会推送此事件：
	* 仅当账户信息有变动时(包括资金、仓位、保证金模式等发生变化)，才会推送此事件；
	* 订单状态变化没有引起账户和持仓变化的，不会推送此事件；
	* 每次因持仓变动推送的position 信息，仅包含当前持仓不为0或逐仓仓位保证金不为0的symbol position。

* "FUNDING FEE" 引起的资金余额变化，仅推送简略事件：
	* 当用户某**全仓**持仓发生"FUNDING FEE"时，事件`ACCOUNT_UPDATE`将只会推送相关的用户资产余额信息`B`(仅推送FUNDING FEE 发生相关的资产余额信息)，而不会推送任何持仓信息`P`。
	* 当用户某**逐仓**仓持仓发生"FUNDING FEE"时，事件`ACCOUNT_UPDATE`将只会推送相关的用户资产余额信息`B`(仅推送"FUNDING FEE"所使用的资产余额信息)，和相关的持仓信息`P`(仅推送这笔"FUNDING FEE"发生所在的持仓信息)，其余持仓信息不会被推送。

* 字段"m"代表了事件推出的原因，包含了以下可能类型:
	* DEPOSIT
	* WITHDRAW
	* ORDER
	* FUNDING_FEE
	* WITHDRAW_REJECT
	* ADJUSTMENT
	* INSURANCE_CLEAR
	* ADMIN_DEPOSIT
	* ADMIN_WITHDRAW
	* MARGIN_TRANSFER
	* MARGIN_TYPE_CHANGE
	* ASSET_TRANSFER
	* OPTIONS_PREMIUM_FEE
	* OPTIONS_SETTLE_PROFIT
	* AUTO_EXCHANGE

* 字段"bc"代表了钱包余额的改变量，即 balance change，但注意其不包含仓位盈亏及交易手续费。

## 订单/交易 更新推送

> **Payload:**

```javascript
{
  
  "e":"ORDER_TRADE_UPDATE",			// 事件类型
  "E":1568879465651,				// 事件时间
  "T":1568879465650,				// 撮合时间
  "o":{								
    "s":"BTCUSDT",					// 交易对
    "c":"TEST",						// 客户端自定订单ID
      // 特殊的自定义订单ID:
      // "autoclose-"开头的字符串: 系统强平订单
      // "adl_autoclose": ADL自动减仓订单
    "S":"SELL",						// 订单方向
    "o":"TRAILING_STOP_MARKET",	// 订单类型
    "f":"GTC",						// 有效方式
    "q":"0.001",					// 订单原始数量
    "p":"0",						// 订单原始价格
    "ap":"0",						// 订单平均价格
    "sp":"7103.04",					// 条件订单触发价格，对追踪止损单无效
    "x":"NEW",						// 本次事件的具体执行类型
    "X":"NEW",						// 订单的当前状态
    "i":8886774,					// 订单ID
    "l":"0",						// 订单末次成交量
    "z":"0",						// 订单累计已成交量
    "L":"0",						// 订单末次成交价格
    "N": "USDT",                 	// 手续费资产类型
    "n": "0",                    	// 手续费数量
    "T":1568879465651,				// 成交时间
    "t":0,							// 成交ID
    "b":"0",						// 买单净值
    "a":"9.91",						// 卖单净值
    "m": false,					    // 该成交是作为挂单成交吗？
    "R":false	,				    // 是否是只减仓单
    "wt": "CONTRACT_PRICE",	        // 触发价类型
    "ot": "TRAILING_STOP_MARKET",	// 原始订单类型
    "ps":"LONG"						// 持仓方向
    "cp":false,						// 是否为触发平仓单; 仅在条件订单情况下会推送此字段
    "AP":"7476.89",					// 追踪止损激活价格, 仅在追踪止损单时会推送此字段
    "cr":"5.0",						// 追踪止损回调比例, 仅在追踪止损单时会推送此字段
    "rp":"0"							// 该交易实现盈亏
    
  }
  
}
```


当有新订单创建、订单有新成交或者新的状态变化时会推送此类事件
事件类型统一为 `ORDER_TRADE_UPDATE`

**订单方向**

* BUY 买入
* SELL 卖出

**订单类型**

* MARKET  市价单
* LIMIT	限价单
* STOP		止损单
* TAKE_PROFIT 止盈单
* LIQUIDATION 强平单

**本次事件的具体执行类型**

* NEW
* CANCELED		已撤
* CALCULATED		
* EXPIRED			订单失效
* TRADE			交易
	

**订单状态**

* NEW
* PARTIALLY_FILLED    
* FILLED
* CANCELED
* EXPIRED
* NEW_INSURANCE		风险保障基金(强平)
* NEW_ADL				自动减仓序列(强平)

**有效方式:**

* GTC 
* IOC
* FOK
* GTX


## 杠杆倍数等账户配置 更新推送

> **Payload:**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // 事件类型
    "E":1611646737479,		           // 事件时间
    "T":1611646737476,		           // 撮合时间
    "ac":{								
    "s":"BTCUSDT",					   // 交易对
    "l":25						       // 杠杆倍数
     
    }
}  
 
```

> **Or**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // 事件类型
    "E":1611646737479,		           // 事件时间
    "T":1611646737476,		           // 撮合时间
    "ai":{							   // 用户账户配置
    "j":true						   // 联合保证金状态
    }
}  
```

当账户配置发生变化时会推送此类事件类型统一为`ACCOUNT_CONFIG_UPDATE `

当交易对杠杆倍数发生变化时推送消息体会包含对象`ac`表示交易对账户配置，其中`s`代表具体的交易对，`l`代表杠杆倍数

当用户联合保证金状态发生变化时推送消息体会包含对象`ai`表示用户账户配置，其中`j`代表用户联合保证金状态



# 错误代码

> error JSON payload:
 
```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

错误由两部分组成：错误代码和消息。 代码是通用的，但是消息可能会有所不同。


## 10xx - 常规服务器或网络问题
> -1000 UNKNOWN
 * An unknown error occured while processing the request.
 * 处理请求时发生未知错误。

> -1001 DISCONNECTED
 * Internal error; unable to process your request. Please try again.
 * 内部错误; 无法处理您的请求。 请再试一次.

> -1002 UNAUTHORIZED
 * You are not authorized to execute this request.
 * 您无权执行此请求。

> -1003 TOO_MANY_REQUESTS
 * Too many requests queued.
 * 排队的请求过多。
 * Too many requests; please use the websocket for live updates.
 * 请求权重过多； 请使用websocket获取最新更新。
 * Too many requests; current limit is %s requests per minute. Please use the websocket for live updates to avoid polling the API.
 * 请求权重过多； 当前限制为每分钟％s请求权重。 请使用websocket进行实时更新，以避免轮询API。
 * Way too many requests; IP banned until %s. Please use the websocket for live updates to avoid bans.
 * 请求权重过多； IP被禁止，直到％s。 请使用websocket进行实时更新，以免被禁。
 
> -1004 DUPLICATE_IP
 * This IP is already on the white list
 * IP地址已经在白名单

> -1005 NO_SUCH_IP
 * No such IP has been white listed
 * 白名单上没有此IP地址
 
> -1006 UNEXPECTED_RESP
 * An unexpected response was received from the message bus. Execution status unknown.
 * 从消息总线收到意外的响应。执行状态未知。

> -1007 TIMEOUT
 * Timeout waiting for response from backend server. Send status unknown; execution status unknown.
 * 等待后端服务器响应超时。 发送状态未知； 执行状态未知。

> -1014 UNKNOWN_ORDER_COMPOSITION
 * Unsupported order combination.
 * 不支持当前的下单参数组合

> -1015 TOO_MANY_ORDERS
 * Too many new orders.
 * 新订单太多。
 * Too many new orders; current limit is %s orders per %s.
 * 新订单太多； 当前限制为每％s ％s个订单。

> -1016 SERVICE_SHUTTING_DOWN
 * This service is no longer available.
 * 该服务不可用。

> -1020 UNSUPPORTED_OPERATION
 * This operation is not supported.
 * 不支持此操作。

> -1021 INVALID_TIMESTAMP
 * Timestamp for this request is outside of the recvWindow.
  * 此请求的时间戳在recvWindow之外。
 * Timestamp for this request was 1000ms ahead of the server's time.
 * 此请求的时间戳比服务器时间提前1000毫秒。

> -1022 INVALID_SIGNATURE
 * Signature for this request is not valid.
 * 此请求的签名无效。

> -1023 START_TIME_GREATER_THAN_END_TIME
 * Start time is greater than end time.
 * 参数里面的开始时间在结束时间之后


## 11xx - Request issues
> -1100 ILLEGAL_CHARS
 * Illegal characters found in a parameter.
 * 在参数中发现非法字符。
 * Illegal characters found in parameter '%s'; legal range is '%s'.
 * 在参数`％s`中发现非法字符； 合法范围是`％s`。

> -1101 TOO_MANY_PARAMETERS
 * Too many parameters sent for this endpoint.
 * 为此端点发送的参数太多。
 * Too many parameters; expected '%s' and received '%s'.
 * 参数太多；预期为`％s`并收到了`％s`。
 * Duplicate values for a parameter detected.
 * 检测到的参数值重复。

> -1102 MANDATORY_PARAM_EMPTY_OR_MALFORMED
 * A mandatory parameter was not sent, was empty/null, or malformed.
 * 未发送强制性参数，该参数为空/空或格式错误。
 * Mandatory parameter '%s' was not sent, was empty/null, or malformed.
 * 强制参数`％s`未发送，为空/空或格式错误。
 * Param '%s' or '%s' must be sent, but both were empty/null!
 * 必须发送参数`％s`或`％s`，但两者均为空！

> -1103 UNKNOWN_PARAM
 * An unknown parameter was sent.
 * 发送了未知参数。

> -1104 UNREAD_PARAMETERS
 * Not all sent parameters were read.
 * 并非所有发送的参数都被读取。
 * Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.
 * 并非所有发送的参数都被读取； 读取了`％s`参数，但被发送了`％s`。

> -1105 PARAM_EMPTY
 * A parameter was empty.
 * 参数为空。
 * Parameter '%s' was empty.
 * 参数`％s`为空。

> -1106 PARAM_NOT_REQUIRED
 * A parameter was sent when not required.
 * 发送了不需要的参数。
 * Parameter '%s' sent when not required.
 * 发送了不需要参数`％s`。

> -1111 BAD_PRECISION
 * Precision is over the maximum defined for this asset.
 * 精度超过为此资产定义的最大值。

> -1112 NO_DEPTH
 * No orders on book for symbol.
 * 交易对没有挂单。
 
> -1114 TIF_NOT_REQUIRED
 * TimeInForce parameter sent when not required.
 * 发送的`TimeInForce`参数不需要。

> -1115 INVALID_TIF
 * Invalid timeInForce.
 * 无效的`timeInForce`

> -1116 INVALID_ORDER_TYPE
 * Invalid orderType.
 * 无效订单类型。

> -1117 INVALID_SIDE
 * Invalid side.
 * 无效买卖方向。

> -1118 EMPTY_NEW_CL_ORD_ID
 * New client order ID was empty.
 * 新的客户订单ID为空。

> -1119 EMPTY_ORG_CL_ORD_ID
 * Original client order ID was empty.
 * 客户自定义的订单ID为空。

> -1120 BAD_INTERVAL
 * Invalid interval.
 * 无效时间间隔。

> -1121 BAD_SYMBOL
 * Invalid symbol.
 * 无效的交易对。

> -1125 INVALID_LISTEN_KEY
 * This listenKey does not exist.
 * 此`listenKey`不存在。

> -1127 MORE_THAN_XX_HOURS
 * Lookup interval is too big.
 * 查询间隔太大。
 * More than %s hours between startTime and endTime.
 * 从开始时间到结束时间之间超过`％s`小时。

> -1128 OPTIONAL_PARAMS_BAD_COMBO
 * Combination of optional parameters invalid.
 * 可选参数组合无效。

> -1130 INVALID_PARAMETER
 * Invalid data sent for a parameter.
 * 发送的参数为无效数据。
 * Data sent for parameter '%s' is not valid.
 * 发送参数`％s`的数据无效。

> -1136 INVALID_NEW_ORDER_RESP_TYPE
 * Invalid newOrderRespType.
 * 无效的 newOrderRespType。


## 20xx - Processing Issues
> -2010 NEW_ORDER_REJECTED
 * NEW_ORDER_REJECTED
 * 新订单被拒绝

> -2011 CANCEL_REJECTED
 * CANCEL_REJECTED
 * 取消订单被拒绝

> -2013 NO_SUCH_ORDER
 * Order does not exist.
 * 订单不存在。

> -2014 BAD_API_KEY_FMT
 * API-key format invalid.
 * API-key 格式无效。

> -2015 REJECTED_MBX_KEY
 * Invalid API-key, IP, or permissions for action.
 * 无效的API密钥，IP或操作权限。

> -2016 NO_TRADING_WINDOW
 * No trading window could be found for the symbol. Try ticker/24hrs instead.
 * 找不到该交易对的交易窗口。 尝试改为24小时自动报价。

> -2018 BALANCE_NOT_SUFFICIENT
 * Balance is insufficient.
 * 余额不足

> -2019 MARGIN_NOT_SUFFICIEN
 * Margin is insufficient.
 * 杠杆账户余额不足

> -2020 UNABLE_TO_FILL
 * Unable to fill.
 * 无法成交

> -2021 ORDER_WOULD_IMMEDIATELY_TRIGGER
 * Order would immediately trigger.
 * 订单可能被立刻触发

> -2022 REDUCE_ONLY_REJECT
 * ReduceOnly Order is rejected.
 * `ReduceOnly`订单被拒绝

> -2023 USER_IN_LIQUIDATION
 * User in liquidation mode now.
 * 用户正处于被强平模式

> -2024 POSITION_NOT_SUFFICIENT
 * Position is not sufficient.
 * 持仓不足

> -2025 MAX_OPEN_ORDER_EXCEEDED
 * Reach max open order limit.
 * 挂单量达到上限

> -2026 REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED
 * This OrderType is not supported when reduceOnly.
 * 当前订单类型不支持`reduceOnly`

> -2027 MAX_LEVERAGE_RATIO
 * Exceeded the maximum allowable position at current leverage.
 * 挂单或持仓超出当前初始杠杆下的最大值

> -2028 MIN_LEVERAGE_RATIO
 * Leverage is smaller than permitted: insufficient margin balance.
 * 调整初始杠杆过低，导致可用余额不足 

## 40xx - Filters and other Issues
> -4000 INVALID_ORDER_STATUS
 * Invalid order status.
 * 订单状态不正确

> -4001 PRICE_LESS_THAN_ZERO
 * Price less than 0.
 * 价格小于0

> -4002 PRICE_GREATER_THAN_MAX_PRICE
 * Price greater than max price.
 * 价格超过最大值
 
> -4003 QTY_LESS_THAN_ZERO
 * Quantity less than zero.
 * 数量小于0

> -4004 QTY_LESS_THAN_MIN_QTY
 * Quantity less than min quantity.
 * 数量小于最小值
 
> -4005 QTY_GREATER_THAN_MAX_QTY
 * Quantity greater than max quantity.
 * 数量大于最大值

> -4006 STOP_PRICE_LESS_THAN_ZERO
 * Stop price less than zero. 
 * 触发价小于最小值
 
> -4007 STOP_PRICE_GREATER_THAN_MAX_PRICE
 * Stop price greater than max price.
 * 触发价大于最大值

> -4008 TICK_SIZE_LESS_THAN_ZERO
 * Tick size less than zero.
 * 价格精度小于0

> -4009 MAX_PRICE_LESS_THAN_MIN_PRICE
 * Max price less than min price.
 * 最大价格小于最小价格

> -4010 MAX_QTY_LESS_THAN_MIN_QTY
 * Max qty less than min qty.
 * 最大数量小于最小数量

> -4011 STEP_SIZE_LESS_THAN_ZERO
 * Step size less than zero.
 * 步进值小于0

> -4012 MAX_NUM_ORDERS_LESS_THAN_ZERO
 * Max num orders less than zero.
 * 最大订单量小于0

> -4013 PRICE_LESS_THAN_MIN_PRICE
 * Price less than min price.
 * 价格小于最小价格

> -4014 PRICE_NOT_INCREASED_BY_TICK_SIZE
 * Price not increased by tick size.
 * 价格增量不是价格精度的倍数。
 
> -4015 INVALID_CL_ORD_ID_LEN
 * Client order id is not valid.
 * 客户订单ID有误。
 * Client order id length should not be more than 36 chars
 * 客户订单ID长度应该不多于36字符

> -4016 PRICE_HIGHTER_THAN_MULTIPLIER_UP
 * Price is higher than mark price multiplier cap.

> -4017 MULTIPLIER_UP_LESS_THAN_ZERO
 * Multiplier up less than zero.
 * 价格上限小于0

> -4018 MULTIPLIER_DOWN_LESS_THAN_ZERO
 * Multiplier down less than zero.
 * 价格下限小于0

> -4019 COMPOSITE_SCALE_OVERFLOW
 * Composite scale too large.

> -4020 TARGET_STRATEGY_INVALID
 * Target strategy invalid for orderType '%s',reduceOnly '%b'.
 * 目标策略值不适合`%s`订单状态, 只减仓`%b`。

> -4021 INVALID_DEPTH_LIMIT
 * Invalid depth limit.
 * 深度信息的`limit`值不正确。
 * '%s' is not valid depth limit.
 * `%s`不是合理的深度信息的`limit`值。

> -4022 WRONG_MARKET_STATUS
 * market status sent is not valid.
 * 发送的市场状态不正确。
 
> -4023 QTY_NOT_INCREASED_BY_STEP_SIZE
 * Qty not increased by step size.
 * 数量的递增值不是步进值的倍数。

> -4024 PRICE_LOWER_THAN_MULTIPLIER_DOWN
 * Price is lower than mark price multiplier floor.

> -4025 MULTIPLIER_DECIMAL_LESS_THAN_ZERO
 * Multiplier decimal less than zero.

> -4026 COMMISSION_INVALID
 * Commission invalid.
 * 收益值不正确
 * `%s` less than zero.
 * `%s`少于0
 * `%s` absolute value greater than `%s`
 * `%s`绝对值大于`%s`

> -4027 INVALID_ACCOUNT_TYPE
 * Invalid account type.
 * 账户类型不正确。

> -4028 INVALID_LEVERAGE
 * Invalid leverage
 * 杠杆倍数不正确
 * Leverage `%s` is not valid
 * 杠杆`%s`不正确
 * Leverage `%s` already exist with `%s`
 * 杠杆`%s`已经存在于`%s`

> -4029 INVALID_TICK_SIZE_PRECISION
 * Tick size precision is invalid.
 * 价格精度小数点位数不正确。

> -4030 INVALID_STEP_SIZE_PRECISION
 * Step size precision is invalid.
 * 步进值小数点位数不正确。

> -4031 INVALID_WORKING_TYPE
 * Invalid parameter working type
 * 不正确的参数类型
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

> -4032 EXCEED_MAX_CANCEL_ORDER_SIZE
 * Exceed maximum cancel order size.
 * 超过可以取消的最大订单量。
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

> -4033 INSURANCE_ACCOUNT_NOT_FOUND
 * Insurance account not found.
 * 风险保障基金账号没找到。

> -4044 INVALID_BALANCE_TYPE
 * Balance Type is invalid.
 * 余额类型不正确。

> -4045 MAX_STOP_ORDER_EXCEEDED
 * Reach max stop order limit.
 * 达到止损单的上限。

> -4046 NO_NEED_TO_CHANGE_MARGIN_TYPE
 * No need to change margin type.
 * 不需要切换仓位模式。

> -4047 THERE_EXISTS_OPEN_ORDERS
 * Margin type cannot be changed if there exists open orders.
 * 如果有挂单，仓位模式不能切换。

> -4048 THERE_EXISTS_QUANTITY
 * Margin type cannot be changed if there exists position.
 * 如果有仓位，仓位模式不能切换。

> -4049 ADD_ISOLATED_MARGIN_REJECT
 * Add margin only support for isolated position.

> -4050 CROSS_BALANCE_INSUFFICIENT
 * Cross balance insufficient.
 * 全仓余额不足。

> -4051 ISOLATED_BALANCE_INSUFFICIENT
 * Isolated balance insufficient.
 * 逐仓余额不足。

> -4052 NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN
 * No need to change auto add margin.

> -4053 AUTO_ADD_CROSSED_MARGIN_REJECT
 * Auto add margin only support for isolated position.
 * 自动增加保证金只适用于逐仓。

> -4054 ADD_ISOLATED_MARGIN_NO_POSITION_REJECT
 * Cannot add position margin: position is 0.
 * 不能增加逐仓保证金: 持仓为0

> -4055 AMOUNT_MUST_BE_POSITIVE
 * Amount must be positive.
 * 数量必须是正整数

> -4056 INVALID_API_KEY_TYPE
 * Invalid api key type.
 * API key的类型不正确

> -4057 INVALID_RSA_PUBLIC_KEY
 * Invalid api public key
 * API key不正确

> -4058 MAX_PRICE_TOO_LARGE
 * maxPrice and priceDecimal too large,please check.
 * maxPrice和priceDecimal太大，请检查。

> -4059 NO_NEED_TO_CHANGE_POSITION_SIDE
 * No need to change position side.
 * 无需变更仓位方向

> -4060 INVALID_POSITION_SIDE
 * Invalid position side.
 * 仓位方向不正确。

> -4061 POSITION_SIDE_NOT_MATCH
 * Order's position side does not match user's setting.
 * 订单的持仓方向和用户设置不一致。

> -4062 REDUCE_ONLY_CONFLICT
 * Invalid or improper reduceOnly value.
 * 仅减仓的设置不正确。

> -4063 INVALID_OPTIONS_REQUEST_TYPE
 * Invalid options request type
 * 无效的期权请求类型

> -4064 INVALID_OPTIONS_TIME_FRAME
 * Invalid options time frame
 * 无效的期权时间窗口

> -4065 INVALID_OPTIONS_AMOUNT
 * Invalid options amount
 * 无效的期权数量

> -4066 INVALID_OPTIONS_EVENT_TYPE
 * Invalid options event type
 * 无效的期权事件类型

> -4067 POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS
 * Position side cannot be changed if there exists open orders.
 * 如果有挂单，无法修改仓位方向。

> -4068 POSITION_SIDE_CHANGE_EXISTS_QUANTITY
 * Position side cannot be changed if there exists position.
 * 如果有仓位, 无法修改仓位方向。

> -4069 INVALID_OPTIONS_PREMIUM_FEE
 * Invalid options premium fee
 * 无效的期权费

> -4070 INVALID_CL_OPTIONS_ID_LEN
 * Client options id is not valid.
 * 客户的期权ID不合法
 * Client options id length should be less than 32 chars
 * 客户的期权ID长度应该小于32个字符

> -4071 INVALID_OPTIONS_DIRECTION
 * Invalid options direction
 * 期权的方向无效

> -4072 OPTIONS_PREMIUM_NOT_UPDATE
 * premium fee is not updated, reject order
 * 期权费没有更新

> -4073 OPTIONS_PREMIUM_INPUT_LESS_THAN_ZERO
 * input premium fee is less than 0, reject order
 * 输入的期权费小于0

> -4074 OPTIONS_AMOUNT_BIGGER_THAN_UPPER
 * Order amount is bigger than upper boundary or less than 0, reject order

> -4075 OPTIONS_PREMIUM_OUTPUT_ZERO
 * output premium fee is less than 0, reject order

> -4076 OPTIONS_PREMIUM_TOO_DIFF
 * original fee is too much higher than last fee
 * 期权的费用比之前的费用高 

> -4077 OPTIONS_PREMIUM_REACH_LIMIT
 * place order amount has reached to limit, reject order
 * 下单的数量达到上限

> -4078 OPTIONS_COMMON_ERROR
 * options internal error
 * 期权内部系统错误

> -4079 INVALID_OPTIONS_ID
 * invalid options id
 * invalid options id: %s
 * duplicate options id %d for user %d
 * 期权ID无效

> -4080 OPTIONS_USER_NOT_FOUND
 * user not found
 * user not found with id: %s
 * 用户找不到

> -4081 OPTIONS_NOT_FOUND
 * options not found
 * options not found with id: %s
 * 期权找不到

> -4082 INVALID_BATCH_PLACE_ORDER_SIZE
 * Invalid number of batch place orders.
 * Invalid number of batch place orders: %s
 * 批量下单的数量不正确

> -4083 PLACE_BATCH_ORDERS_FAIL
 * Fail to place batch orders.
 * 无法批量下单

> -4084 UPCOMING_METHOD
 * Method is not allowed currently. Upcoming soon.
 * 方法不支持

> -4085 INVALID_NOTIONAL_LIMIT_COEF
 * Invalid notional limit coefficient
 * 期权的有限系数不正确

> -4086 INVALID_PRICE_SPREAD_THRESHOLD
 * Invalid price spread threshold
 * 无效的价差阀值
 
> -4087 REDUCE_ONLY_ORDER_PERMISSION
 * User can only place reduce only order
 * 用户只能下仅减仓订单

> -4088 NO_PLACE_ORDER_PERMISSION
 * User can not place order currently
 * 用户当前不能下单

> -4104 INVALID_CONTRACT_TYPE
 * Invalid contract type
 * 无效的合约类型

> -4114 INVALID_CLIENT_TRAN_ID_LEN
 * clientTranId  is not valid
 * clientTranId不正确
 * Client tran id length should be less than 64 chars
 * 客户的tranId长度应该小于64个字符

> -4115 DUPLICATED_CLIENT_TRAN_ID
 * clientTranId  is duplicated
 *  clientTranId重复
 * Client tran id should be unique within 7 days
 * 客户的tranId应在7天内唯一

> -4118 REDUCE_ONLY_MARGIN_CHECK_FAILED
 * ReduceOnly Order Failed. Please check your existing position and open orders
 * 仅减仓订单失败。请检查现有的持仓和挂单
 
> -4131 MARKET_ORDER_REJECT
 * The counterparty's best price does not meet the PERCENT_PRICE filter limit
 * 交易对手的最高价格未达到PERCENT_PRICE过滤器限制

> -4135 INVALID_ACTIVATION_PRICE
 * Invalid activation price
 * 无效的激活价格

> -4137 QUANTITY_EXISTS_WITH_CLOSE_POSITION
 * Quantity must be zero with closePosition equals true
 * 数量必须为0，当closePosition为true时

> -4138 REDUCE_ONLY_MUST_BE_TRUE
 * Reduce only must be true with closePosition equals true
 * Reduce only 必须为true，当closePosition为true时

> -4139 ORDER_TYPE_CANNOT_BE_MKT
 * Order type can not be market if it's unable to cancel
 * 订单类型不能为市价单如果不能取消

> -4140 INVALID_OPENING_POSITION_STATUS
 * Invalid symbol status for opening position
 * 无效的交易对状态

> -4141 SYMBOL_ALREADY_CLOSED
 * Symbol is closed
 * 交易对已下架

> -4142 STRATEGY_INVALID_TRIGGER_PRICE
 * REJECT: take profit or stop order will be triggered immediately
 * 拒绝：止盈止损单将立即被触发

> -4144 INVALID_PAIR
 * Invalid pair
 * 无效的pair

> -4161 ISOLATED_LEVERAGE_REJECT_WITH_POSITION
 * Leverage reduction is not supported in Isolated Margin Mode with open positions
 * 逐仓仓位模式下无法降低杠杆

> -4164 MIN_NOTIONAL
 * Order's notional must be no smaller than 5.0 (unless you choose reduce only)
 *  订单的名义价值不可以小于5，除了使用reduce only
 * Order's notional must be no smaller than %s (unless you choose reduce only)
 *  订单的名义价值不可以小于`%s`，除了使用reduce only

> -4165 INVALID_TIME_INTERVAL
 * Invalid time interval
 * 无效的间隔
 * Maximum time interval is %s days
 * 最大的时间间隔为 `%s` 天

> -4183 PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP
 * Price is higher than stop price multiplier cap.
 * 止盈止损订单价格不应高于触发价与报价乘数上限的乘积
 * Limit price can't be higher than %s.
 * 止盈止损订单价格不应高于 `%s`

> -4184 PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN
 * Price is lower than stop price multiplier floor.
 * 止盈止损订单价格不应低于触发价与报价乘数下限的乘积
 * Limit price can't be lower than %s.
 * 止盈止损订单价格不应低于 `%s`


---

# aster-finance-futures-api.md

- [General Info](#general-info)
	- [General API Information](#general-api-information)
		- [HTTP Return Codes](#http-return-codes)
		- [Error Codes and Messages](#error-codes-and-messages)
		- [General Information on Endpoints](#general-information-on-endpoints)
	- [LIMITS](#limits)
		- [IP Limits](#ip-limits)
		- [Order Rate Limits](#order-rate-limits)
	- [Endpoint Security Type](#endpoint-security-type)
	- [SIGNED (TRADE and USER_DATA) Endpoint Security](#signed-trade-and-user_data-endpoint-security)
		- [Timing Security](#timing-security)
		- [SIGNED Endpoint Examples for POST /fapi/v1/order](#signed-endpoint-examples-for-post-fapiv1order)
			- [Example 1: As a query string](#example-1-as-a-query-string)
			- [Example 2: As a request body](#example-2-as-a-request-body)
			- [Example 3: Mixed query string and request body](#example-3-mixed-query-string-and-request-body)
	- [Public Endpoints Info](#public-endpoints-info)
		- [Terminology](#terminology)
		- [ENUM definitions](#enum-definitions)
	- [Filters](#filters)
		- [Symbol filters](#symbol-filters)
			- [PRICE_FILTER](#price_filter)
			- [LOT_SIZE](#lot_size)
			- [MARKET_LOT_SIZE](#market_lot_size)
			- [MAX_NUM_ORDERS](#max_num_orders)
			- [MAX_NUM_ALGO_ORDERS](#max_num_algo_orders)
			- [PERCENT_PRICE](#percent_price)
			- [MIN_NOTIONAL](#min_notional)
- [Market Data Endpoints](#market-data-endpoints)
	- [Test Connectivity](#test-connectivity)
	- [Check Server Time](#check-server-time)
	- [Exchange Information](#exchange-information)
	- [Order Book](#order-book)
	- [Recent Trades List](#recent-trades-list)
	- [Old Trades Lookup (MARKET_DATA)](#old-trades-lookup-market_data)
	- [Compressed/Aggregate Trades List](#compressedaggregate-trades-list)
	- [Kline/Candlestick Data](#klinecandlestick-data)
	- [Index Price Kline/Candlestick Data](#index-price-klinecandlestick-data)
	- [Mark Price Kline/Candlestick Data](#mark-price-klinecandlestick-data)
	- [Mark Price](#mark-price)
	- [Get Funding Rate History](#get-funding-rate-history)
    - [Get Funding Rate Config](#get-funding-rate-config)
	- [24hr Ticker Price Change Statistics](#24hr-ticker-price-change-statistics)
	- [Symbol Price Ticker](#symbol-price-ticker)
	- [Symbol Order Book Ticker](#symbol-order-book-ticker)
- [Websocket Market Streams](#websocket-market-streams)
	- [Live Subscribing/Unsubscribing to streams](#live-subscribingunsubscribing-to-streams)
		- [Subscribe to a stream](#subscribe-to-a-stream)
		- [Unsubscribe to a stream](#unsubscribe-to-a-stream)
		- [Listing Subscriptions](#listing-subscriptions)
		- [Setting Properties](#setting-properties)
		- [Retrieving Properties](#retrieving-properties)
		- [Error Messages](#error-messages)
	- [Aggregate Trade Streams](#aggregate-trade-streams)
	- [Mark Price Stream](#mark-price-stream)
	- [Mark Price Stream for All market](#mark-price-stream-for-all-market)
	- [Kline/Candlestick Streams](#klinecandlestick-streams)
	- [Individual Symbol Mini Ticker Stream](#individual-symbol-mini-ticker-stream)
	- [All Market Mini Tickers Stream](#all-market-mini-tickers-stream)
	- [Individual Symbol Ticker Streams](#individual-symbol-ticker-streams)
	- [All Market Tickers Streams](#all-market-tickers-streams)
	- [Individual Symbol Book Ticker Streams](#individual-symbol-book-ticker-streams)
	- [All Book Tickers Stream](#all-book-tickers-stream)
	- [Liquidation Order Streams](#liquidation-order-streams)
	- [All Market Liquidation Order Streams](#all-market-liquidation-order-streams)
	- [Partial Book Depth Streams](#partial-book-depth-streams)
	- [Diff. Book Depth Streams](#diff-book-depth-streams)
	- [How to manage a local order book correctly](#how-to-manage-a-local-order-book-correctly)
- [Account/Trades Endpoints](#accounttrades-endpoints)
	- [Change Position Mode(TRADE)](#change-position-modetrade)
	- [Get Current Position Mode(USER_DATA)](#get-current-position-modeuser_data)
	- [Change Multi-Assets Mode (TRADE)](#change-multi-assets-mode-trade)
	- [Get Current Multi-Assets Mode (USER_DATA)](#get-current-multi-assets-mode-user_data)
	- [New Order  (TRADE)](#new-order--trade)
	- [Place Multiple Orders  (TRADE)](#place-multiple-orders--trade)
	- [Transfer Between Futures And Spot (USER_DATA)](#transfer-between-futures-and-spot-user_data)
	- [Query Order (USER_DATA)](#query-order-user_data)
	- [Cancel Order (TRADE)](#cancel-order-trade)
	- [Cancel All Open Orders (TRADE)](#cancel-all-open-orders-trade)
	- [Cancel Multiple Orders (TRADE)](#cancel-multiple-orders-trade)
	- [Auto-Cancel All Open Orders (TRADE)](#auto-cancel-all-open-orders-trade)
	- [Query Current Open Order (USER_DATA)](#query-current-open-order-user_data)
	- [Current All Open Orders (USER_DATA)](#current-all-open-orders-user_data)
	- [All Orders (USER_DATA)](#all-orders-user_data)
	- [Futures Account Balance V2 (USER_DATA)](#futures-account-balance-v2-user_data)
	- [Account Information V2 (USER_DATA)](#account-information-v2-user_data)
	- [Change Initial Leverage (TRADE)](#change-initial-leverage-trade)
	- [Change Margin Type (TRADE)](#change-margin-type-trade)
	- [Modify Isolated Position Margin (TRADE)](#modify-isolated-position-margin-trade)
	- [Get Position Margin Change History (TRADE)](#get-position-margin-change-history-trade)
	- [Position Information V2 (USER_DATA)](#position-information-v2-user_data)
	- [Account Trade List (USER_DATA)](#account-trade-list-user_data)
	- [Get Income History(USER_DATA)](#get-income-historyuser_data)
	- [Notional and Leverage Brackets (USER_DATA)](#notional-and-leverage-brackets-user_data)
	- [Position ADL Quantile Estimation (USER_DATA)](#position-adl-quantile-estimation-user_data)
	- [User's Force Orders (USER_DATA)](#users-force-orders-user_data)
	- [User Commission Rate (USER_DATA)](#user-commission-rate-user_data)
- [User Data Streams](#user-data-streams)
	- [Start User Data Stream (USER_STREAM)](#start-user-data-stream-user_stream)
	- [Keepalive User Data Stream (USER_STREAM)](#keepalive-user-data-stream-user_stream)
	- [Close User Data Stream (USER_STREAM)](#close-user-data-stream-user_stream)
	- [Event: User Data Stream Expired](#event-user-data-stream-expired)
	- [Event: Margin Call](#event-margin-call)
	- [Event: Balance and Position Update](#event-balance-and-position-update)
	- [Event: Order Update](#event-order-update)
	- [Event: Account Configuration Update previous Leverage Update](#event-account-configuration-update-previous-leverage-update)
- [Error Codes](#error-codes)
	- [10xx - General Server or Network issues](#10xx---general-server-or-network-issues)
	- [11xx - Request issues](#11xx---request-issues)
	- [20xx - Processing Issues](#20xx---processing-issues)
	- [40xx - Filters and other Issues](#40xx---filters-and-other-issues)

# General Info

## General API Information

* Some endpoints will require an API Key. Please refer to [this page](https://www.asterdex.com/)
* The base endpoint is: **https://fapi.asterdex.com**
* All endpoints return either a JSON object or array.
* Data is returned in **ascending** order. Oldest first, newest last.
* All time and timestamp related fields are in milliseconds.
* All data types adopt definition in JAVA.

### HTTP Return Codes

* HTTP `4XX` return codes are used for for malformed requests;
  the issue is on the sender's side.
* HTTP `403` return code is used when the WAF Limit (Web Application Firewall) has been violated.  
* HTTP `429` return code is used when breaking a request rate limit.
* HTTP `418` return code is used when an IP has been auto-banned for continuing to send requests after receiving `429` codes.
* HTTP `5XX` return codes are used for internal errors; the issue is on
  Aster's side.   
* HTTP `503` return code is used when the API successfully sent the message but not get a response within the timeout period.   
  It is important to **NOT** treat this as a failure operation; the execution status is
  **UNKNOWN** and could have been a success.

### Error Codes and Messages

* Any endpoint can return an ERROR

> ***The error payload is as follows:***
 
```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

* Specific error codes and messages defined in [Error Codes](#error-codes).

### General Information on Endpoints

* For `GET` endpoints, parameters must be sent as a `query string`.
* For `POST`, `PUT`, and `DELETE` endpoints, the parameters may be sent as a
  `query string` or in the `request body` with content type
  `application/x-www-form-urlencoded`. You may mix parameters between both the
  `query string` and `request body` if you wish to do so.
* Parameters may be sent in any order.
* If a parameter sent in both the `query string` and `request body`, the
  `query string` parameter will be used.

## LIMITS
* The `/fapi/v1/exchangeInfo` `rateLimits` array contains objects related to the exchange's `RAW_REQUEST`, `REQUEST_WEIGHT`, and `ORDER` rate limits. These are further defined in the `ENUM definitions` section under `Rate limiters (rateLimitType)`.
* A `429` will be returned when either rate limit is violated.

<aside class="notice">
Aster Finance has the right to further tighten the rate limits on users with intent to attack.
</aside>

### IP Limits
* Every request will contain `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` in the response headers which has the current used weight for the IP for all request rate limiters defined.
* Each route has a `weight` which determines for the number of requests each endpoint counts for. Heavier endpoints and endpoints that do operations on multiple symbols will have a heavier `weight`.
* When a 429 is received, it's your obligation as an API to back off and not spam the API.
* **Repeatedly violating rate limits and/or failing to back off after receiving 429s will result in an automated IP ban (HTTP status 418).**
* IP bans are tracked and **scale in duration** for repeat offenders, **from 2 minutes to 3 days**.
* **The limits on the API are based on the IPs, not the API keys.**

<aside class="notice">
It is strongly recommended to use websocket stream for getting data as much as possible, which can not only ensure the timeliness of the message, but also reduce the access restriction pressure caused by the request.
</aside>

### Order Rate Limits
* Every order response will contain a `X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)` header which has the current order count for the account for all order rate limiters defined.
* Rejected/unsuccessful orders are not guaranteed to have `X-MBX-ORDER-COUNT-**` headers in the response.
* **The order rate limit is counted against each account**.

## Endpoint Security Type
* Each endpoint has a security type that determines the how you will
  interact with it.
* API-keys are passed into the Rest API via the `X-MBX-APIKEY`
  header.
* API-keys and secret-keys **are case sensitive**.
* API-keys can be configured to only access certain types of secure endpoints.
 For example, one API-key could be used for TRADE only, while another API-key
 can access everything except for TRADE routes.
* By default, API-keys can access all secure routes.

Security Type | Description
------------ | ------------
NONE | Endpoint can be accessed freely.
TRADE | Endpoint requires sending a valid API-Key and signature.
USER_DATA | Endpoint requires sending a valid API-Key and signature.
USER_STREAM | Endpoint requires sending a valid API-Key.
MARKET_DATA | Endpoint requires sending a valid API-Key.


* `TRADE` and `USER_DATA` endpoints are `SIGNED` endpoints.

## SIGNED (TRADE and USER_DATA) Endpoint Security
* `SIGNED` endpoints require an additional parameter, `signature`, to be
  sent in the  `query string` or `request body`.
* Endpoints use `HMAC SHA256` signatures. The `HMAC SHA256 signature` is a keyed `HMAC SHA256` operation.
  Use your `secretKey` as the key and `totalParams` as the value for the HMAC operation.
* The `signature` is **not case sensitive**.
* Please make sure the `signature` is the end part of your `query string` or `request body`.
* `totalParams` is defined as the `query string` concatenated with the
  `request body`.

### Timing Security
* A `SIGNED` endpoint also requires a parameter, `timestamp`, to be sent which
  should be the millisecond timestamp of when the request was created and sent.
* An additional parameter, `recvWindow`, may be sent to specify the number of
  milliseconds after `timestamp` the request is valid for. If `recvWindow`
  is not sent, **it defaults to 5000**.
  
> The logic is as follows:

```javascript
  if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow){
    // process request
  } 
  else {
    // reject request
  }
```

**Serious trading is about timing.** Networks can be unstable and unreliable,
which can lead to requests taking varying amounts of time to reach the
servers. With `recvWindow`, you can specify that the request must be
processed within a certain number of milliseconds or be rejected by the
server.

<aside class="notice">
It is recommended to use a small recvWindow of 5000 or less!
</aside>

### SIGNED Endpoint Examples for POST /fapi/v1/order
Here is a step-by-step example of how to send a vaild signed payload from the
Linux command line using `echo`, `openssl`, and `curl`.

Key | Value
------------ | ------------
apiKey | dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83
secretKey | 2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9


Parameter | Value
------------ | ------------
symbol | BTCUSDT
side | BUY
type | LIMIT
timeInForce | GTC
quantity | 1
price | 9000
recvWindow | 5000
timestamp | 1591702613943


#### Example 1: As a query string

> **Example 1**

>  **HMAC SHA256 signature:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```
> **curl command:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi/asterdex.com/fapi/v1/order?symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943&signature= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```
* **queryString:** 

	symbol=BTCUSDT  
	&side=BUY   
	&type=LIMIT  
	&timeInForce=GTC   
	&quantity=1  
	&price=9000   
	&recvWindow=5000   
	&timestamp=1591702613943




#### Example 2: As a request body

> **Example 2**

> **HMAC SHA256 signature:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```


> **curl command:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi/asterdex.com/fapi/v1/order' -d 'symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943&signature= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```

* **requestBody:**

	symbol=BTCUSDT   
	&side=BUY   
	&type=LIMIT   
	&timeInForce=GTC   
	&quantity=1   
	&price=9000  
	&recvWindow=5000   
	&timestamp=1591702613943



#### Example 3: Mixed query string and request body

> **Example 3**

> **HMAC SHA256 signature:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```

> **curl command:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi/asterdex.com/fapi/v1/order?symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC' -d 'quantity=1&price=9000&recvWindow=5000&timestamp=1591702613943&signature=3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```

* **queryString:** symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC
* **requestBody:** quantity=1&price=9000&recvWindow=5000&timestamp= 1591702613943


Note that the signature is different in example 3.     
There is no & between "GTC" and "quantity=1".


## Public Endpoints Info
### Terminology
* `base asset` refers to the asset that is the `quantity` of a symbol.
* `quote asset` refers to the asset that is the `price` of a symbol.


### ENUM definitions

**Symbol type:**

* FUTURE

**Contract type (contractType):**

* PERPETUAL 

**Contract status(contractStatus，status):**

* PENDING_TRADING 
* TRADING 
* PRE_SETTLE	
* SETTLING	
* CLOSE	


**Order status (status):**

* NEW
* PARTIALLY_FILLED
* FILLED
* CANCELED
* REJECTED
* EXPIRED

**Order types (orderTypes, type):**

* LIMIT 
* MARKET 
* STOP 
* STOP_MARKET 
* TAKE_PROFIT 
* TAKE_PROFIT_MARKET 
* TRAILING_STOP_MARKET

**Order side (side):**

* BUY
* SELL

**Position side (positionSide):**

* BOTH 
* LONG 
* SHORT 

**Time in force (timeInForce):**

* GTC - Good Till Cancel
* IOC - Immediate or Cancel
* FOK - Fill or Kill
* GTX - Good Till Crossing	(Post Only)	
* HIDDEN - HIDDEN This type of order is not visible in the order book

**Working Type (workingType)**

* MARK_PRICE
* CONTRACT_PRICE 

**Response Type (newOrderRespType)**

* ACK
* RESULT


**Kline/Candlestick chart intervals:**

m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Rate limiters (rateLimitType)**

> REQUEST_WEIGHT

```javascript
  {
  	"rateLimitType": "REQUEST_WEIGHT",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 2400
  }
```

> ORDERS

```javascript
  {
  	"rateLimitType": "ORDERS",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 1200
   }
```


* REQUEST_WEIGHT

* ORDERS


**Rate limit intervals (interval)**

* MINUTE




## Filters
Filters define trading rules on a symbol or an exchange.

### Symbol filters
#### PRICE_FILTER

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "PRICE_FILTER",
    "minPrice": "0.00000100",
    "maxPrice": "100000.00000000",
    "tickSize": "0.00000100"
  }
```

The `PRICE_FILTER` defines the `price` rules for a symbol. There are 3 parts:

* `minPrice` defines the minimum `price`/`stopPrice` allowed; disabled on `minPrice` == 0.
* `maxPrice` defines the maximum `price`/`stopPrice` allowed; disabled on `maxPrice` == 0.
* `tickSize` defines the intervals that a `price`/`stopPrice` can be increased/decreased by; disabled on `tickSize` == 0.

Any of the above variables can be set to 0, which disables that rule in the `price filter`. In order to pass the `price filter`, the following must be true for `price`/`stopPrice` of the enabled rules:

* `price` >= `minPrice` 
* `price` <= `maxPrice`
* (`price`-`minPrice`) % `tickSize` == 0


#### LOT_SIZE

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

The `LOT_SIZE` filter defines the `quantity` (aka "lots" in auction terms) rules for a symbol. There are 3 parts:

* `minQty` defines the minimum `quantity` allowed.
* `maxQty` defines the maximum `quantity` allowed.
* `stepSize` defines the intervals that a `quantity` can be increased/decreased by.

In order to pass the `lot size`, the following must be true for `quantity`:

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0



#### MARKET_LOT_SIZE


> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MARKET_LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

The `MARKET_LOT_SIZE` filter defines the `quantity` (aka "lots" in auction terms) rules for `MARKET` orders on a symbol. There are 3 parts:

* `minQty` defines the minimum `quantity` allowed.
* `maxQty` defines the maximum `quantity` allowed.
* `stepSize` defines the intervals that a `quantity` can be increased/decreased by.

In order to pass the `market lot size`, the following must be true for `quantity`:

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0


#### MAX_NUM_ORDERS

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MAX_NUM_ORDERS",
    "limit": 200
  }
```

The `MAX_NUM_ORDERS` filter defines the maximum number of orders an account is allowed to have open on a symbol.

Note that both "algo" orders and normal orders are counted for this filter.


#### MAX_NUM_ALGO_ORDERS

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MAX_NUM_ALGO_ORDERS",
    "limit": 100
  }
```

The `MAX_NUM_ALGO_ORDERS ` filter defines the maximum number of all kinds of algo orders an account is allowed to have open on a symbol.

The algo orders include `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`, and `TRAILING_STOP_MARKET` orders.


#### PERCENT_PRICE

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "PERCENT_PRICE",
    "multiplierUp": "1.1500",
    "multiplierDown": "0.8500",
    "multiplierDecimal": 4
  }
```

The `PERCENT_PRICE` filter defines valid range for a price based on the mark price.

In order to pass the `percent price`, the following must be true for `price`:

* BUY: `price` <= `markPrice` * `multiplierUp`
* SELL: `price` >= `markPrice` * `multiplierDown`


#### MIN_NOTIONAL

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MIN_NOTIONAL",
    "notional": "1"
  }
```

The `MIN_NOTIONAL` filter defines the minimum notional value allowed for an order on a symbol.
An order's notional value is the `price` * `quantity`.
Since `MARKET` orders have no price, the mark price is used.



---

# Market Data Endpoints

## Test Connectivity


> **Response:**

```javascript
{}
```


``
GET /fapi/v1/ping
``

Test connectivity to the Rest API.

**Weight:**
1

**Parameters:**
NONE



## Check Server Time

> **Response:**

```javascript
{
  "serverTime": 1499827319559
}
```

``
GET /fapi/v1/time
``

Test connectivity to the Rest API and get the current server time.

**Weight:**
1

**Parameters:**
NONE


## Exchange Information

> **Response:**

```javascript
{
	"exchangeFilters": [],
 	"rateLimits": [
 		{
 			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 2400,
   			"rateLimitType": "REQUEST_WEIGHT" 
   		},
  		{
  			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 1200,
   			"rateLimitType": "ORDERS"
   		}
   	],
 	"serverTime": 1565613908500,    // Ignore please. If you want to check current server time, please check via "GET /fapi/v1/time"
 	"assets": [ // assets information
 		{
 			"asset": "BUSD",
   			"marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
   			"autoAssetExchange": 0 // auto-exchange threshold in Multi-Assets margin mode
   		},
 		{
 			"asset": "USDT",
   			"marginAvailable": true,
   			"autoAssetExchange": 0
   		},
 		{
 			"asset": "BTC",
   			"marginAvailable": false,
   			"autoAssetExchange": null
   		}
   	],
 	"symbols": [
 		{
 			"symbol": "DOGEUSDT",
 			"pair": "DOGEUSDT",
 			"contractType": "PERPETUAL",
 			"deliveryDate": 4133404800000,
 			"onboardDate": 1598252400000,
 			"status": "TRADING",
 			"maintMarginPercent": "2.5000",   // ignore
 			"requiredMarginPercent": "5.0000",  // ignore
 			"baseAsset": "BLZ", 
 			"quoteAsset": "USDT",
 			"marginAsset": "USDT",
 			"pricePrecision": 5,	// please do not use it as tickSize
 			"quantityPrecision": 0, // please do not use it as stepSize
 			"baseAssetPrecision": 8,
 			"quotePrecision": 8, 
 			"underlyingType": "COIN",
 			"underlyingSubType": ["STORAGE"],
 			"settlePlan": 0,
 			"triggerProtect": "0.15", // threshold for algo order with "priceProtect"
 			"filters": [
 				{
 					"filterType": "PRICE_FILTER",
     				"maxPrice": "300",
     				"minPrice": "0.0001", 
     				"tickSize": "0.0001"
     			},
    			{
    				"filterType": "LOT_SIZE", 
     				"maxQty": "10000000",
     				"minQty": "1",
     				"stepSize": "1"
     			},
    			{
    				"filterType": "MARKET_LOT_SIZE",
     				"maxQty": "590119",
     				"minQty": "1",
     				"stepSize": "1"
     			},
     			{
    				"filterType": "MAX_NUM_ORDERS",
    				"limit": 200
  				},
  				{
    				"filterType": "MAX_NUM_ALGO_ORDERS",
    				"limit": 100
  				},
  				{
  					"filterType": "MIN_NOTIONAL",
  					"notional": "1", 
  				},
  				{
    				"filterType": "PERCENT_PRICE",
    				"multiplierUp": "1.1500",
    				"multiplierDown": "0.8500",
    				"multiplierDecimal": 4
    			}
   			],
 			"OrderType": [
   				"LIMIT",
   				"MARKET",
   				"STOP",
   				"STOP_MARKET",
   				"TAKE_PROFIT",
   				"TAKE_PROFIT_MARKET",
   				"TRAILING_STOP_MARKET" 
   			],
   			"timeInForce": [
   				"GTC", 
   				"IOC", 
   				"FOK", 
   				"GTX",
				"HIDDEN"
 			],
 			"liquidationFee": "0.010000",	// liquidation fee rate
   			"marketTakeBound": "0.30",	// the max price difference rate( from mark price) a market order can make
 		}
   	],
	"timezone": "UTC" 
}

```

``
GET /fapi/v1/exchangeInfo
``

Current exchange trading rules and symbol information

**Weight:**
1

**Parameters:**
NONE




## Order Book


> **Response:**

```javascript
{
  "lastUpdateId": 1027024,
  "E": 1589436922972,   // Message output time
  "T": 1589436922959,   // Transaction time
  "bids": [
    [
      "4.00000000",     // PRICE
      "431.00000000"    // QTY
    ]
  ],
  "asks": [
    [
      "4.00000200",
      "12.00000000"
    ]
  ]
}
```

``
GET /fapi/v1/depth
``

**Weight:**

Adjusted based on the limit:


Limit | Weight
------------ | ------------
5, 10, 20, 50 | 2
100 | 5
500 | 10
1000 | 20

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | Default 500; Valid limits:[5, 10, 20, 50, 100, 500, 1000]




## Recent Trades List

> **Response:**

```javascript
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "48.00",
    "time": 1499865549590,
    "isBuyerMaker": true,
  }
]
```

``
GET /fapi/v1/trades
``

Get recent market trades

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | Default 500; max 1000.

* Market trades means trades filled in the order book. Only market trades will be returned, which means the insurance fund trades and ADL trades won't be returned.


## Old Trades Lookup (MARKET_DATA)

> **Response:**

```javascript
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "8000.00",
    "time": 1499865549590,
    "isBuyerMaker": true,
  }
]
```

``
GET /fapi/v1/historicalTrades
``

Get older market historical trades.

**Weight:**
20

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | Default 500; max 1000.
fromId | LONG | NO | TradeId to fetch from. Default gets most recent trades.

* Market trades means trades filled in the order book. Only market trades will be returned, which means the insurance fund trades and ADL trades won't be returned.


## Compressed/Aggregate Trades List

> **Response:**

```javascript
[
  {
    "a": 26129,         // Aggregate tradeId
    "p": "0.01633102",  // Price
    "q": "4.70443515",  // Quantity
    "f": 27781,         // First tradeId
    "l": 27781,         // Last tradeId
    "T": 1498793709153, // Timestamp
    "m": true,          // Was the buyer the maker?
  }
]
```

``
GET /fapi/v1/aggTrades
``

Get compressed, aggregate market trades. Market trades that fill at the time, from the same order, with the same price will have the quantity aggregated.

**Weight:**
20

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
fromId | LONG | NO | ID to get aggregate trades from INCLUSIVE.
startTime | LONG | NO | Timestamp in ms to get aggregate trades from INCLUSIVE.
endTime | LONG | NO | Timestamp in ms to get aggregate trades until INCLUSIVE.
limit | INT | NO | Default 500; max 1000.

* If both startTime and endTime are sent, time between startTime and endTime must be less than 1 hour.
* If fromId, startTime, and endTime are not sent, the most recent aggregate trades will be returned.
* Only market trades will be aggregated and returned, which means the insurance fund trades and ADL trades won't be aggregated.



## Kline/Candlestick Data


> **Response:**

```javascript
[
  [
    1499040000000,      // Open time
    "0.01634790",       // Open
    "0.80000000",       // High
    "0.01575800",       // Low
    "0.01577100",       // Close
    "148976.11427815",  // Volume
    1499644799999,      // Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "17928899.62484339" // Ignore.
  ]
]
```

``
GET /fapi/v1/klines
``

Kline/candlestick bars for a symbol.
Klines are uniquely identified by their open time.

**Weight:** based on parameter `LIMIT`

LIMIT | weight
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
interval | ENUM | YES |
startTime | LONG | NO |
endTime | LONG | NO |
limit | INT | NO | Default 500; max 1500.

* If startTime and endTime are not sent, the most recent klines are returned.


## Index Price Kline/Candlestick Data

> **Response:**

```javascript
[
  [
    1591256400000,      	// Open time
    "9653.69440000",    	// Open
    "9653.69640000",     	// High
    "9651.38600000",     	// Low
    "9651.55200000",     	// Close (or latest price)
    "0	", 					// Ignore
    1591256459999,      	// Close time
    "0",    				// Ignore
    60,                		// Number of bisic data
    "0",    				// Ignore
    "0",      				// Ignore
    "0" 					// Ignore
  ]
]
```

``
GET /fapi/v1/indexPriceKlines
``

Kline/candlestick bars for the index price of a pair.

Klines are uniquely identified by their open time.

**Weight:** based on parameter `LIMIT`

LIMIT | weight
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
pair    	| STRING | YES      | 
interval  | ENUM   | YES      | 
startTime | LONG   | NO       | 
endTime   | LONG   | NO       | 
limit     | INT    | NO       |  Default 500; max 1500.

* If startTime and endTime are not sent, the most recent klines are returned.


## Mark Price Kline/Candlestick Data

> **Response:**

```javascript
[
  [
    1591256460000,     		// Open time
    "9653.29201333",    	// Open
    "9654.56401333",     	// High
    "9653.07367333",     	// Low
    "9653.07367333",     	// Close (or latest price)
    "0	", 					// Ignore
    1591256519999,      	// Close time
    "0",    				// Ignore
    60,                	 	// Number of bisic data
    "0",    				// Ignore
    "0",      			 	// Ignore
    "0" 					// Ignore
  ]
]
```

``
GET /fapi/v1/markPriceKlines
``

Kline/candlestick bars for the mark price of a symbol.

Klines are uniquely identified by their open time.


**Weight:** based on parameter `LIMIT`

LIMIT | weight
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol   	| STRING | YES      | 
interval  | ENUM   | YES      | 
startTime | LONG   | NO       | 
endTime   | LONG   | NO       | 
limit     | INT    | NO       |  Default 500; max 1500.

* If startTime and endTime are not sent, the most recent klines are returned.


## Mark Price


> **Response:**

```javascript
{
	"symbol": "BTCUSDT",
	"markPrice": "11793.63104562",	// mark price
	"indexPrice": "11781.80495970",	// index price
	"estimatedSettlePrice": "11781.16138815", // Estimated Settle Price, only useful in the last hour before the settlement starts.
	"lastFundingRate": "0.00038246",  // This is the lasted funding rate
	"nextFundingTime": 1597392000000,
	"interestRate": "0.00010000",
	"time": 1597370495002
}
```

> **OR (when symbol not sent)**

```javascript
[
	{
	    "symbol": "BTCUSDT",
	    "markPrice": "11793.63104562",	// mark price
	    "indexPrice": "11781.80495970",	// index price
	    "estimatedSettlePrice": "11781.16138815", // Estimated Settle Price, only useful in the last hour before the settlement starts.
	    "lastFundingRate": "0.00038246",  // This is the lasted funding rate
	    "nextFundingTime": 1597392000000,
	    "interestRate": "0.00010000",	
	    "time": 1597370495002
	}
]
```


``
GET /fapi/v1/premiumIndex
``

Mark Price and Funding Rate

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |



## Get Funding Rate History

> **Response:**

```javascript
[
	{
    	"symbol": "BTCUSDT",
    	"fundingRate": "-0.03750000",
    	"fundingTime": 1570608000000,
	},
	{
   		"symbol": "BTCUSDT",
    	"fundingRate": "0.00010000",
    	"fundingTime": 1570636800000,
	}
]
```

``
GET /fapi/v1/fundingRate
``


**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
startTime | LONG | NO | Timestamp in ms to get funding rate from INCLUSIVE.
endTime | LONG | NO | Timestamp in ms to get funding rate  until INCLUSIVE.
limit | INT | NO | Default 100; max 1000


* If `startTime` and `endTime` are not sent, the most recent `limit` datas are returned.
* If the number of data between `startTime` and `endTime` is larger than `limit`, return as `startTime` + `limit`.
* In ascending order.

## Get Funding Rate Config

> **Response:**

```javascript
[
	{
		"symbol": "INJUSDT",
		"interestRate": "0.00010000",
		"time": 1756197479000,
		"fundingIntervalHours": 8,
		"fundingFeeCap": 0.03,
		"fundingFeeFloor": -0.03
	},
	{
		"symbol": "ZORAUSDT",
		"interestRate": "0.00005000",
		"time": 1756197479000,
		"fundingIntervalHours": 4,
		"fundingFeeCap": 0.02,
		"fundingFeeFloor": -0.02
	}
]
```

``
GET /fapi/v1/fundingInfo
``


**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |



## 24hr Ticker Price Change Statistics

> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "priceChange": "-94.99999800",
  "priceChangePercent": "-95.960",
  "weightedAvgPrice": "0.29628482",
  "prevClosePrice": "0.10002000",
  "lastPrice": "4.00000200",
  "lastQty": "200.00000000",
  "openPrice": "99.00000000",
  "highPrice": "100.00000000",
  "lowPrice": "0.10000000",
  "volume": "8913.30000000",
  "quoteVolume": "15.30000000",
  "openTime": 1499783499040,
  "closeTime": 1499869899040,
  "firstId": 28385,   // First tradeId
  "lastId": 28460,    // Last tradeId
  "count": 76         // Trade count
}
```

> OR

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"priceChange": "-94.99999800",
  		"priceChangePercent": "-95.960",
  		"weightedAvgPrice": "0.29628482",
  		"prevClosePrice": "0.10002000",
  		"lastPrice": "4.00000200",
  		"lastQty": "200.00000000",
  		"openPrice": "99.00000000",
  		"highPrice": "100.00000000",
  		"lowPrice": "0.10000000",
  		"volume": "8913.30000000",
  		"quoteVolume": "15.30000000",
  		"openTime": 1499783499040,
  		"closeTime": 1499869899040,
  		"firstId": 28385,   // First tradeId
  		"lastId": 28460,    // Last tradeId
  		"count": 76         // Trade count
	}
]
```

``
GET /fapi/v1/ticker/24hr
``

24 hour rolling window price change statistics.    
**Careful** when accessing this with no symbol.

**Weight:**   
1 for a single symbol;    
**40** when the symbol parameter is omitted

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |

* If the symbol is not sent, tickers for all symbols will be returned in an array.



## Symbol Price Ticker

> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "price": "6000.01",
  "time": 1589437530011   // Transaction time
}
```

> OR


```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"price": "6000.01",
  		"time": 1589437530011
	}
]
```

``
GET /fapi/v1/ticker/price
``

Latest price for a symbol or symbols.

**Weight:**   
1 for a single symbol;    
**2** when the symbol parameter is omitted

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |

* If the symbol is not sent, prices for all symbols will be returned in an array.



## Symbol Order Book Ticker


> **Response:**

```javascript
{
  "symbol": "BTCUSDT",
  "bidPrice": "4.00000000",
  "bidQty": "431.00000000",
  "askPrice": "4.00000200",
  "askQty": "9.00000000",
  "time": 1589437530011   // Transaction time
}
```

> OR


```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"bidPrice": "4.00000000",
  		"bidQty": "431.00000000",
  		"askPrice": "4.00000200",
  		"askQty": "9.00000000",
  		"time": 1589437530011
	}
]
```

``
GET /fapi/v1/ticker/bookTicker
``

Best price/qty on the order book for a symbol or symbols.

**Weight:**   
1 for a single symbol;    
**2** when the symbol parameter is omitted

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |

* If the symbol is not sent, bookTickers for all symbols will be returned in an array.



# Websocket Market Streams

* The baseurl for websocket is **wss://fstream.asterdex.com**
* Streams can be access either in a single raw stream or a combined stream
* Raw streams are accessed at **/ws/\<streamName\>**
* Combined streams are accessed at **/stream?streams=\<streamName1\>/\<streamName2\>/\<streamName3\>**
* Combined stream events are wrapped as follows: **{"stream":"\<streamName\>","data":\<rawPayload\>}**
* All symbols for streams are **lowercase**
* A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
* The websocket server will send a `ping frame` every 5 minutes. If the websocket server does not receive a `pong frame` back from the connection within a 15 minute period, the connection will be disconnected. Unsolicited `pong frames` are allowed.
* WebSocket connections have a limit of 10 incoming messages per second.
* A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.
* A single connection can listen to a maximum of **200** streams.
* Considering the possible data latency from RESTful endpoints during an extremely volatile market, it is highly recommended to get the order status, position, etc from the Websocket user data stream.


## Live Subscribing/Unsubscribing to streams

* The following data can be sent through the websocket instance in order to subscribe/unsubscribe from streams. Examples can be seen below.
* The `id` used in the JSON payloads is an unsigned INT used as an identifier to uniquely identify the messages going back and forth.

### Subscribe to a stream

> **Response**

  ```javascript
  {
    "result": null,
    "id": 1
  }
  ```

* **Request**

  	{    
    	"method": "SUBSCRIBE",    
    	"params":     
    	[   
      	"btcusdt@aggTrade",    
      	"btcusdt@depth"     
    	],    
    	"id": 1   
  	}



### Unsubscribe to a stream

> **Response**
  
  ```javascript
  {
    "result": null,
    "id": 312
  }
  ```


* **Request**

  {   
    "method": "UNSUBSCRIBE",    
    "params":     
    [    
      "btcusdt@depth"   
    ],    
    "id": 312   
  }



### Listing Subscriptions

> **Response**
  
  ```javascript
  {
    "result": [
      "btcusdt@aggTrade"
    ],
    "id": 3
  }
  ```


* **Request**

  {   
    "method": "LIST_SUBSCRIPTIONS",    
    "id": 3   
  }     
 


### Setting Properties
Currently, the only property can be set is to set whether `combined` stream payloads are enabled are not.
The combined property is set to `false` when connecting using `/ws/` ("raw streams") and `true` when connecting using `/stream/`.

> **Response**
  
  ```javascript
  {
    "result": null,
    "id": 5
  }
  ```

* **Request**

  {    
    "method": "SET_PROPERTY",    
    "params":     
    [   
      "combined",    
      true   
    ],    
    "id": 5   
  }




### Retrieving Properties

> **Response**

  ```javascript
  {
    "result": true, // Indicates that combined is set to true.
    "id": 2
  }
  ```
  
* **Request**
  
  {   
    "method": "GET_PROPERTY",    
    "params":     
    [   
      "combined"   
    ],    
    "id": 2   
  }   
 



### Error Messages

Error Message | Description
---|---
{"code": 0, "msg": "Unknown property"} | Parameter used in the `SET_PROPERTY` or `GET_PROPERTY` was invalid
{"code": 1, "msg": "Invalid value type: expected Boolean"} | Value should only be `true` or `false`
{"code": 2, "msg": "Invalid request: property name must be a string"}| Property name provided was invalid
{"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"}| Parameter `id` had to be provided or the value provided in the `id` parameter is an unsupported type
{"code": 2, "msg": "Invalid request: unknown variant %s, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28"} | Possible typo in the provided method or provided method was neither of the expected values
{"code": 2, "msg": "Invalid request: too many parameters"}| Unnecessary parameters provided in the data
{"code": 2, "msg": "Invalid request: property name must be a string"} | Property name was not provided
{"code": 2, "msg": "Invalid request: missing field `method` at line 1 column 73"} | `method` was not provided in the data
{"code":3,"msg":"Invalid JSON: expected value at line %s column %s"} | JSON data sent has incorrect syntax.






## Aggregate Trade Streams


> **Payload:**

```javascript
{
  "e": "aggTrade",  // Event type
  "E": 123456789,   // Event time
  "s": "BTCUSDT",    // Symbol
  "a": 5933014,		// Aggregate trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "f": 100,         // First trade ID
  "l": 105,         // Last trade ID
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
}
```

The Aggregate Trade Streams push market trade information that is aggregated for a single taker order every 100 milliseconds.

**Stream Name:**     
``<symbol>@aggTrade``

**Update Speed:** 100ms

* Only market trades will be aggregated, which means the insurance fund trades and ADL trades won't be aggregated.

## Mark Price Stream

> **Payload:**

```javascript
  {
    "e": "markPriceUpdate",  	// Event type
    "E": 1562305380000,      	// Event time
    "s": "BTCUSDT",          	// Symbol
    "p": "11794.15000000",   	// Mark price
    "i": "11784.62659091",		// Index price
    "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
    "r": "0.00038167",       	// Funding rate
    "T": 1562306400000       	// Next funding time
  }
```

Mark price and funding rate for a single symbol pushed every 3 seconds or every second.

**Stream Name:**     
``<symbol>@markPrice`` or ``<symbol>@markPrice@1s``

**Update Speed:** 3000ms or 1000ms



## Mark Price Stream for All market

> **Payload:**

```javascript
[ 
  {
    "e": "markPriceUpdate",  	// Event type
    "E": 1562305380000,      	// Event time
    "s": "BTCUSDT",          	// Symbol
    "p": "11185.87786614",   	// Mark price
    "i": "11784.62659091"		// Index price
    "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
    "r": "0.00030000",       	// Funding rate
    "T": 1562306400000       	// Next funding time
  }
]
```

Mark price and funding rate for all symbols pushed every 3 seconds or every second.

**Stream Name:**     
``!markPrice@arr`` or ``!markPrice@arr@1s``

**Update Speed:** 3000ms or 1000ms



## Kline/Candlestick Streams


> **Payload:**

```javascript
{
  "e": "kline",     // Event type
  "E": 123456789,   // Event time
  "s": "BTCUSDT",    // Symbol
  "k": {
    "t": 123400000, // Kline start time
    "T": 123460000, // Kline close time
    "s": "BTCUSDT",  // Symbol
    "i": "1m",      // Interval
    "f": 100,       // First trade ID
    "L": 200,       // Last trade ID
    "o": "0.0010",  // Open price
    "c": "0.0020",  // Close price
    "h": "0.0025",  // High price
    "l": "0.0015",  // Low price
    "v": "1000",    // Base asset volume
    "n": 100,       // Number of trades
    "x": false,     // Is this kline closed?
    "q": "1.0000",  // Quote asset volume
    "V": "500",     // Taker buy base asset volume
    "Q": "0.500",   // Taker buy quote asset volume
    "B": "123456"   // Ignore
  }
}
```

The Kline/Candlestick Stream push updates to the current klines/candlestick every 250 milliseconds (if existing).

**Kline/Candlestick chart intervals:**

m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Stream Name:**     
``<symbol>@kline_<interval>``

**Update Speed:** 250ms


## Individual Symbol Mini Ticker Stream


> **Payload:**

```javascript
  {
    "e": "24hrMiniTicker",  // Event type
    "E": 123456789,         // Event time
    "s": "BTCUSDT",         // Symbol
    "c": "0.0025",          // Close price
    "o": "0.0010",          // Open price
    "h": "0.0025",          // High price
    "l": "0.0010",          // Low price
    "v": "10000",           // Total traded base asset volume
    "q": "18"               // Total traded quote asset volume
  }
```

24hr rolling window mini-ticker statistics for a single symbol. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before.

**Stream Name:**     
``<symbol>@miniTicker``

**Update Speed:** 500ms




## All Market Mini Tickers Stream


> **Payload:**

```javascript
[  
  {
    "e": "24hrMiniTicker",  // Event type
    "E": 123456789,         // Event time
    "s": "BTCUSDT",         // Symbol
    "c": "0.0025",          // Close price
    "o": "0.0010",          // Open price
    "h": "0.0025",          // High price
    "l": "0.0010",          // Low price
    "v": "10000",           // Total traded base asset volume
    "q": "18"               // Total traded quote asset volume
  }
]
```

24hr rolling window mini-ticker statistics for all symbols. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before. Note that only tickers that have changed will be present in the array.

**Stream Name:**     
``!miniTicker@arr``

**Update Speed:** 1000ms



## Individual Symbol Ticker Streams


> **Payload:**

```javascript
{
  "e": "24hrTicker",  // Event type
  "E": 123456789,     // Event time
  "s": "BTCUSDT",     // Symbol
  "p": "0.0015",      // Price change
  "P": "250.00",      // Price change percent
  "w": "0.0018",      // Weighted average price
  "c": "0.0025",      // Last price
  "Q": "10",          // Last quantity
  "o": "0.0010",      // Open price
  "h": "0.0025",      // High price
  "l": "0.0010",      // Low price
  "v": "10000",       // Total traded base asset volume
  "q": "18",          // Total traded quote asset volume
  "O": 0,             // Statistics open time
  "C": 86400000,      // Statistics close time
  "F": 0,             // First trade ID
  "L": 18150,         // Last trade Id
  "n": 18151          // Total number of trades
}
```

24hr rollwing window ticker statistics for a single symbol. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before.

**Stream Name:**     
``<symbol>@ticker``

**Update Speed:** 500ms



## All Market Tickers Streams


> **Payload:**

```javascript
[
	{
	  "e": "24hrTicker",  // Event type
	  "E": 123456789,     // Event time
	  "s": "BTCUSDT",     // Symbol
	  "p": "0.0015",      // Price change
	  "P": "250.00",      // Price change percent
	  "w": "0.0018",      // Weighted average price
	  "c": "0.0025",      // Last price
	  "Q": "10",          // Last quantity
	  "o": "0.0010",      // Open price
	  "h": "0.0025",      // High price
	  "l": "0.0010",      // Low price
	  "v": "10000",       // Total traded base asset volume
	  "q": "18",          // Total traded quote asset volume
	  "O": 0,             // Statistics open time
	  "C": 86400000,      // Statistics close time
	  "F": 0,             // First trade ID
	  "L": 18150,         // Last trade Id
	  "n": 18151          // Total number of trades
	}
]
```

24hr rollwing window ticker statistics for all symbols. These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before. Note that only tickers that have changed will be present in the array.

**Stream Name:**     
``!ticker@arr``

**Update Speed:** 1000ms





## Individual Symbol Book Ticker Streams

> **Payload:**

```javascript
{
  "e":"bookTicker",			// event type
  "u":400900217,     		// order book updateId
  "E": 1568014460893,  		// event time
  "T": 1568014460891,  		// transaction time
  "s":"BNBUSDT",     		// symbol
  "b":"25.35190000", 		// best bid price
  "B":"31.21000000", 		// best bid qty
  "a":"25.36520000", 		// best ask price
  "A":"40.66000000"  		// best ask qty
}
```


Pushes any update to the best bid or ask's price or quantity in real-time for a specified symbol.

**Stream Name:** `<symbol>@bookTicker`

**Update Speed:** Real-time





## All Book Tickers Stream

> **Payload:**

```javascript
{
  // Same as <symbol>@bookTicker payload
}
```

Pushes any update to the best bid or ask's price or quantity in real-time for all symbols.

**Stream Name:** `!bookTicker`

**Update Speed:** Real-time



## Liquidation Order Streams

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // Event Type
	"E":1568014460893,                  // Event Time
	"o":{
	
		"s":"BTCUSDT",                   // Symbol
		"S":"SELL",                      // Side
		"o":"LIMIT",                     // Order Type
		"f":"IOC",                       // Time in Force
		"q":"0.014",                     // Original Quantity
		"p":"9910",                      // Price
		"ap":"9910",                     // Average Price
		"X":"FILLED",                    // Order Status
		"l":"0.014",                     // Order Last Filled Quantity
		"z":"0.014",                     // Order Filled Accumulated Quantity
		"T":1568014460893,          	 // Order Trade Time
	
	}

}
```

The Liquidation Order Snapshot Streams push force liquidation order information for specific symbol.

For each symbol，only the latest one liquidation order within 1000ms will be pushed as the snapshot. If no liquidation happens in the interval of 1000ms, no stream will be pushed.

**Stream Name:**  ``<symbol>@forceOrder``

**Update Speed:** 1000ms



## All Market Liquidation Order Streams

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // Event Type
	"E":1568014460893,                  // Event Time
	"o":{
	
		"s":"BTCUSDT",                   // Symbol
		"S":"SELL",                      // Side
		"o":"LIMIT",                     // Order Type
		"f":"IOC",                       // Time in Force
		"q":"0.014",                     // Original Quantity
		"p":"9910",                      // Price
		"ap":"9910",                     // Average Price
		"X":"FILLED",                    // Order Status
		"l":"0.014",                     // Order Last Filled Quantity
		"z":"0.014",                     // Order Filled Accumulated Quantity
		"T":1568014460893,          	 // Order Trade Time
	
	}

}
```

The All Liquidation Order Snapshot Streams push force liquidation order information for all symbols in the market.

For each symbol，only the latest one liquidation order within 1000ms will be pushed as the snapshot. If no liquidation happens in the interval of 1000ms, no stream will be pushed.

**Stream Name:** ``!forceOrder@arr``

**Update Speed:** 1000ms






## Partial Book Depth Streams

> **Payload:**

```javascript
{
  "e": "depthUpdate", // Event type
  "E": 1571889248277, // Event time
  "T": 1571889248276, // Transaction time
  "s": "BTCUSDT",
  "U": 390497796,
  "u": 390497878,
  "pu": 390497794,
  "b": [          // Bids to be updated
    [
      "7403.89",  // Price Level to be
      "0.002"     // Quantity
    ],
    [
      "7403.90",
      "3.906"
    ],
    [
      "7404.00",
      "1.428"
    ],
    [
      "7404.85",
      "5.239"
    ],
    [
      "7405.43",
      "2.562"
    ]
  ],
  "a": [          // Asks to be updated
    [
      "7405.96",  // Price level to be
      "3.340"     // Quantity
    ],
    [
      "7406.63",
      "4.525"
    ],
    [
      "7407.08",
      "2.475"
    ],
    [
      "7407.15",
      "4.800"
    ],
    [
      "7407.20",
      "0.175"
    ]
  ]
}
```

Top **<levels\>** bids and asks, Valid **<levels\>** are 5, 10, or 20.

**Stream Names:** `<symbol>@depth<levels>` OR `<symbol>@depth<levels>@500ms` OR `<symbol>@depth<levels>@100ms`.  

**Update Speed:** 250ms, 500ms or 100ms






## Diff. Book Depth Streams


> **Payload:**

```javascript
{
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 157,           // First update ID in event
  "u": 160,           // Final update ID in event
  "pu": 149,          // Final update Id in last stream(ie `u` in last stream)
  "b": [              // Bids to be updated
    [
      "0.0024",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "0.0026",       // Price level to be updated
      "100"          // Quantity
    ]
  ]
}
```

Bids and asks, pushed every 250 milliseconds, 500 milliseconds, 100 milliseconds (if existing)

**Stream Name:**     
``<symbol>@depth`` OR ``<symbol>@depth@500ms``  OR ``<symbol>@depth@100ms``

**Update Speed:** 250ms, 500ms, 100ms



## How to manage a local order book correctly
1. Open a stream to **wss://fstream.asterdex.com/stream?streams=btcusdt@depth**.
2. Buffer the events you receive from the stream. For same price, latest received update covers the previous one. 
3. Get a depth snapshot from **https://fapi.asterdex.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000** .
4. Drop any event where `u` is < `lastUpdateId` in the snapshot.
5. The first processed event should have `U` <= `lastUpdateId` **AND** `u` >= `lastUpdateId`
6. While listening to the stream, each new event's `pu` should be equal to the previous event's `u`, otherwise initialize the process from step 3.
7. The data in each event is the **absolute** quantity for a price level.
8. If the quantity is 0, **remove** the price level.
9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.



# Account/Trades Endpoints

<aside class="warning">
Considering the possible data latency from RESTful endpoints during an extremely volatile market, it is highly recommended to get the order status, position, etc from the Websocket user data stream.
</aside>

## Change Position Mode(TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/positionSide/dual (HMAC SHA256)
``

Change user's position mode (Hedge Mode or One-way Mode ) on ***EVERY symbol***    

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
dualSidePosition | STRING   | YES      | "true": Hedge Mode; "false": One-way Mode 
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## Get Current Position Mode(USER_DATA)

> **Response:**

```javascript
{
	"dualSidePosition": true // "true": Hedge Mode; "false": One-way Mode
}
```

``
GET /fapi/v1/positionSide/dual (HMAC SHA256)
``

Get user's position mode (Hedge Mode or One-way Mode ) on ***EVERY symbol***    

**Weight:**
30

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## Change Multi-Assets Mode (TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/multiAssetsMargin (HMAC SHA256)
``

Change user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on ***Every symbol***

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
---------- | ------ | -------- | -----------------
multiAssetsMargin | STRING   | YES      | "true": Multi-Assets Mode; "false": Single-Asset Mode
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## Get Current Multi-Assets Mode (USER_DATA)

> **Response:**

```javascript
{
	"multiAssetsMargin": true // "true": Multi-Assets Mode; "false": Single-Asset Mode
}
```

``
GET /fapi/v1/multiAssetsMargin (HMAC SHA256)
``

Get user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on ***Every symbol***    

**Weight:**
30

**Parameters:**

Name | Type | Mandatory | Description
---------- | ------ | -------- | -----------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## New Order  (TRADE)


> **Response:**

```javascript
{
 	"clientOrderId": "testOrder",
 	"cumQty": "0",
 	"cumQuote": "0",
 	"executedQty": "0",
 	"orderId": 22542179,
 	"avgPrice": "0.00000",
 	"origQty": "10",
 	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",		// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"origType": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",	// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",			// callback rate, only return with TRAILING_STOP_MARKET order
 	"updateTime": 1566818724722,
 	"workingType": "CONTRACT_PRICE",
 	"priceProtect": false            // if conditional order trigger is protected	
}
```


``
POST /fapi/v1/order  (HMAC SHA256)
``

Send in a new order.

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
side | ENUM | YES |
positionSide | ENUM	| NO | Default `BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent in Hedge Mode.
type | ENUM | YES |
timeInForce | ENUM | NO |
quantity | DECIMAL | NO | Cannot be sent with `closePosition`=`true`(Close-All)
reduceOnly | STRING | NO | "true" or "false". default "false". Cannot be sent in Hedge Mode; cannot be sent with `closePosition`=`true`
price | DECIMAL | NO |
newClientOrderId | STRING | NO | A unique id among open orders. Automatically generated if not sent. Can only be string following the rule: `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice | DECIMAL | NO | Used with `STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.
closePosition    | STRING  | NO       | `true`, `false`；Close-All，used with `STOP_MARKET` or `TAKE_PROFIT_MARKET`.
activationPrice  | DECIMAL | NO       | Used with `TRAILING_STOP_MARKET` orders, default as the latest price(supporting different `workingType`)
callbackRate     | DECIMAL | NO       | Used with `TRAILING_STOP_MARKET` orders, min 0.1, max 5 where 1 for 1%
workingType | ENUM | NO | stopPrice triggered by: "MARK_PRICE", "CONTRACT_PRICE". Default "CONTRACT_PRICE"
priceProtect | STRING | NO | "TRUE" or "FALSE", default "FALSE". Used with `STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders. 
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", default "ACK"
recvWindow | LONG | NO |
timestamp | LONG | YES |

Additional mandatory parameters based on `type`:

Type | Additional mandatory parameters
------------ | ------------
`LIMIT` | `timeInForce`, `quantity`, `price`
`MARKET` | `quantity`
`STOP/TAKE_PROFIT` | `quantity`,  `price`, `stopPrice`
`STOP_MARKET/TAKE_PROFIT_MARKET` | `stopPrice`
`TRAILING_STOP_MARKET` | `callbackRate`

* Order with type `STOP`,  parameter `timeInForce` can be sent ( default `GTC`).
* Order with type `TAKE_PROFIT`,  parameter `timeInForce` can be sent ( default `GTC`).
* Condition orders will be triggered when:
	
	* If parameter`priceProtect`is sent as true:
		* when price reaches the `stopPrice` ，the difference rate between "MARK_PRICE" and "CONTRACT_PRICE" cannot be larger than the "triggerProtect" of the symbol
		* "triggerProtect" of a symbol can be got from `GET /fapi/v1/exchangeInfo`
		
	* `STOP`, `STOP_MARKET`:
		* BUY: latest price ("MARK_PRICE" or "CONTRACT_PRICE") >= `stopPrice`
		* SELL: latest price ("MARK_PRICE" or "CONTRACT_PRICE") <= `stopPrice`
	* `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`:
		* BUY: latest price ("MARK_PRICE" or "CONTRACT_PRICE") <= `stopPrice`
		* SELL: latest price ("MARK_PRICE" or "CONTRACT_PRICE") >= `stopPrice`
	* `TRAILING_STOP_MARKET`:
		* BUY: the lowest price after order placed <= `activationPrice`, and the latest price >= the lowest price * (1 + `callbackRate`)
		* SELL: the highest price after order placed >= `activationPrice`, and the latest price <= the highest price * (1 - `callbackRate`)

* For `TRAILING_STOP_MARKET`, if you got such error code.  
  ``{"code": -2021, "msg": "Order would immediately trigger."}``    
  means that the parameters you send do not meet the following requirements:
	* BUY: `activationPrice` should be smaller than latest price.
	* SELL: `activationPrice` should be larger than latest price.

* If `newOrderRespType ` is sent as `RESULT` :
	* `MARKET` order: the final FILLED result of the order will be return directly.
	* `LIMIT` order with special `timeInForce`: the final status result of the order(FILLED or EXPIRED) will be returned directly.

* `STOP_MARKET`, `TAKE_PROFIT_MARKET` with `closePosition`=`true`:
	* Follow the same rules for condition orders.
	* If triggered，**close all** current long position( if `SELL`) or current short position( if `BUY`).
	* Cannot be used with `quantity` paremeter
	* Cannot be used with `reduceOnly` parameter
	* In Hedge Mode,cannot be used with `BUY` orders in `LONG` position side. and cannot be used with `SELL` orders in `SHORT` position side


## Place Multiple Orders  (TRADE)


> **Response:**

```javascript
[
	{
	 	"clientOrderId": "testOrder",
	 	"cumQty": "0",
	 	"cumQuote": "0",
	 	"executedQty": "0",
	 	"orderId": 22542179,
	 	"avgPrice": "0.00000",
	 	"origQty": "10",
	 	"price": "0",
	  	"reduceOnly": false,
	  	"side": "BUY",
	  	"positionSide": "SHORT",
	  	"status": "NEW",
	  	"stopPrice": "9300",		// please ignore when order type is TRAILING_STOP_MARKET
	  	"symbol": "BTCUSDT",
	  	"timeInForce": "GTC",
	  	"type": "TRAILING_STOP_MARKET",
	  	"origType": "TRAILING_STOP_MARKET",
	  	"activatePrice": "9020",	// activation price, only return with TRAILING_STOP_MARKET order
	  	"priceRate": "0.3",			// callback rate, only return with TRAILING_STOP_MARKET order
	 	"updateTime": 1566818724722,
	 	"workingType": "CONTRACT_PRICE",
	 	"priceProtect": false            // if conditional order trigger is protected	
	},
	{
		"code": -2022, 
		"msg": "ReduceOnly Order is rejected."
	}
]
```

``
POST /fapi/v1/batchOrders  (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
batchOrders |	LIST<JSON> | 	YES |	order list. Max 5 orders
recvWindow |	LONG |	NO	
timestamp	| LONG | YES	

**Where ``batchOrders`` is the list of order parameters in JSON**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
side | ENUM | YES |
positionSide | ENUM	| NO | Default `BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent with Hedge Mode.
type | ENUM | YES |
timeInForce | ENUM | NO |
quantity | DECIMAL | YES |
reduceOnly | STRING | NO | "true" or "false". default "false".
price | DECIMAL | NO |
newClientOrderId | STRING | NO | A unique id among open orders. Automatically generated if not sent. Can only be string following the rule: `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice | DECIMAL | NO | Used with `STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders.
activationPrice  | DECIMAL | NO       | Used with `TRAILING_STOP_MARKET` orders, default as the latest price(supporting different `workingType`)
callbackRate     | DECIMAL | NO       | Used with `TRAILING_STOP_MARKET` orders, min 0.1, max 4 where 1 for 1%
workingType | ENUM | NO | stopPrice triggered by: "MARK_PRICE", "CONTRACT_PRICE". Default "CONTRACT_PRICE"
priceProtect | STRING | NO | "TRUE" or "FALSE", default "FALSE". Used with `STOP/STOP_MARKET` or `TAKE_PROFIT/TAKE_PROFIT_MARKET` orders. 
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", default "ACK"


* Paremeter rules are same with `New Order`
* Batch orders are processed concurrently, and the order of matching is not guaranteed.
* The order of returned contents for batch orders is the same as the order of the order list.

## Transfer Between Futures And Spot (USER_DATA)

> **Response:**

```javascript
{
    "tranId": 21841, //transaction id
    "status": "SUCCESS" //status
}
```

``
POST /fapi/v1/asset/wallet/transfer (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	amount
asset |	STRING | 	YES |	asset
clientTranId |	STRING | 	YES |	transaction id 
kindType |	STRING | 	YES |	kindType
timestamp	| LONG | YES	|	timestamp

Notes:

* kindType can take the following values:
     FUTURE_SPOT  (futures converted to spot)
	 SPOT_FUTURE  (spot converted to futures)


## Query Order (USER_DATA)


> **Response:**

```javascript
{
  	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected	
}
```

``
GET /fapi/v1/order (HMAC SHA256)
``

Check an order's status.

**Weight:**
1

* These orders will not be found:
	* order status is `CANCELED` or `EXPIRED`, **AND** 
	* order has NO filled trade, **AND**
	* created time + 7 days < current time

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
origClientOrderId | STRING | NO |
recvWindow | LONG | NO |
timestamp | LONG | YES |

Notes:

* Either `orderId` or `origClientOrderId` must be sent.



## Cancel Order (TRADE)

> **Response:**

```javascript
{
 	"clientOrderId": "myOrder1",
 	"cumQty": "0",
 	"cumQuote": "0",
 	"executedQty": "0",
 	"orderId": 283194212,
 	"origQty": "11",
 	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "CANCELED",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
 	"updateTime": 1571110484038,
 	"workingType": "CONTRACT_PRICE",
 	"priceProtect": false            // if conditional order trigger is protected	
}
```

``
DELETE /fapi/v1/order  (HMAC SHA256)
``

Cancel an active order.

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
origClientOrderId | STRING | NO | 
recvWindow | LONG | NO |
timestamp | LONG | YES |

Either `orderId` or `origClientOrderId` must be sent.


## Cancel All Open Orders (TRADE)

> **Response:**

```javascript
{
	"code": "200", 
	"msg": "The operation of cancel all open order is done."
}
```

``
DELETE /fapi/v1/allOpenOrders  (HMAC SHA256)
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
recvWindow | LONG | NO |
timestamp | LONG | YES |


## Cancel Multiple Orders (TRADE)

> **Response:**

```javascript
[
	{
	 	"clientOrderId": "myOrder1",
	 	"cumQty": "0",
	 	"cumQuote": "0",
	 	"executedQty": "0",
	 	"orderId": 283194212,
	 	"origQty": "11",
	 	"origType": "TRAILING_STOP_MARKET",
  		"price": "0",
  		"reduceOnly": false,
  		"side": "BUY",
  		"positionSide": "SHORT",
  		"status": "CANCELED",
  		"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  		"closePosition": false,   // if Close-All
  		"symbol": "BTCUSDT",
  		"timeInForce": "GTC",
  		"type": "TRAILING_STOP_MARKET",
  		"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  		"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
	 	"updateTime": 1571110484038,
	 	"workingType": "CONTRACT_PRICE",
	 	"priceProtect": false            // if conditional order trigger is protected	
	},
	{
		"code": -2011,
		"msg": "Unknown order sent."
	}
]
```

``
DELETE /fapi/v1/batchOrders  (HMAC SHA256)
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderIdList | LIST\<LONG\> | NO | max length 10 <br /> e.g. [1234567,2345678]
origClientOrderIdList | LIST\<STRING\> | NO | max length 10<br /> e.g. ["my_id_1","my_id_2"], encode the double quotes. No space after comma.
recvWindow | LONG | NO |
timestamp | LONG | YES |

Either `orderIdList` or `origClientOrderIdList ` must be sent.




## Auto-Cancel All Open Orders (TRADE)

> **Response:**

```javascript
{
	"symbol": "BTCUSDT", 
	"countdownTime": "100000"
}
```


Cancel all open orders of the specified symbol at the end of the specified countdown.

``
POST /fapi/v1/countdownCancelAll  (HMAC SHA256)
``

**Weight:**
10

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
countdownTime | LONG | YES | countdown time, 1000 for 1 second. 0 to cancel the timer
recvWindow | LONG | NO |
timestamp | LONG | YES |

* The endpoint should be called repeatedly as heartbeats so that the existing countdown time can be canceled and replaced by a new one.

* Example usage:    
	Call this endpoint at 30s intervals with an countdownTime of 120000 (120s).   
	If this endpoint is not called within 120 seconds, all your orders of the specified symbol will be automatically canceled.   
	If this endpoint is called with an countdownTime of 0, the countdown timer will be stopped.
	
* The system will check all countdowns **approximately every 10 milliseconds**, so please note that sufficient redundancy should be considered when using this function. We do not recommend setting the countdown time to be too precise or too small.





## Query Current Open Order (USER_DATA)

> **Response:**

```javascript

{
  	"avgPrice": "0.00000",				
  	"clientOrderId": "abc",				
  	"cumQuote": "0",						
  	"executedQty": "0",					
  	"orderId": 1917641,					
  	"origQty": "0.40",						
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   			// if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order						
  	"updateTime": 1579276756075,		
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected			
}
```

``
GET /fapi/v1/openOrder  (HMAC SHA256)
``


**Weight:** 1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO | 
origClientOrderId | STRING | NO | 
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* Either`orderId` or `origClientOrderId` must be sent
* If the queried order has been filled or cancelled, the error message "Order does not exist" will be returned.




## Current All Open Orders (USER_DATA)

> **Response:**

```javascript
[
  {
  	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected	
  }
]
```

``
GET /fapi/v1/openOrders  (HMAC SHA256)
``

Get all open orders on a symbol. **Careful** when accessing this with no symbol.

**Weight:**
1 for a single symbol; **40** when the symbol parameter is omitted


**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
recvWindow | LONG | NO |
timestamp | LONG | YES |


* If the symbol is not sent, orders for all symbols will be returned in an array.

## All Orders (USER_DATA)


> **Response:**

```javascript
[
  {
   	"avgPrice": "0.00000",
  	"clientOrderId": "abc",
  	"cumQuote": "0",
  	"executedQty": "0",
  	"orderId": 1917641,
  	"origQty": "0.40",
  	"origType": "TRAILING_STOP_MARKET",
  	"price": "0",
  	"reduceOnly": false,
  	"side": "BUY",
  	"positionSide": "SHORT",
  	"status": "NEW",
  	"stopPrice": "9300",				// please ignore when order type is TRAILING_STOP_MARKET
  	"closePosition": false,   // if Close-All
  	"symbol": "BTCUSDT",
  	"time": 1579276756075,				// order time
  	"timeInForce": "GTC",
  	"type": "TRAILING_STOP_MARKET",
  	"activatePrice": "9020",			// activation price, only return with TRAILING_STOP_MARKET order
  	"priceRate": "0.3",					// callback rate, only return with TRAILING_STOP_MARKET order
  	"updateTime": 1579276756075,		// update time
  	"workingType": "CONTRACT_PRICE",
  	"priceProtect": false            // if conditional order trigger is protected	
  }
]
```

``
GET /fapi/v1/allOrders (HMAC SHA256)
``

Get all account orders; active, canceled, or filled.

* These orders will not be found:
	* order status is `CANCELED` or `EXPIRED`, **AND** 
	* order has NO filled trade, **AND**
	* created time + 7 days < current time

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
startTime | LONG | NO |
endTime | LONG | NO |
limit | INT | NO | Default 500; max 1000.
recvWindow | LONG | NO |
timestamp | LONG | YES |

**Notes:**

* If `orderId` is set, it will get orders >= that `orderId`. Otherwise most recent orders are returned.
* The query time period must be less then 7 days( default as the recent 7 days).



## Futures Account Balance V2 (USER_DATA)

> **Response:**

```javascript
[
 	{
 		"accountAlias": "SgsR",    // unique account code
 		"asset": "USDT",  	// asset name
 		"balance": "122607.35137903", // wallet balance
 		"crossWalletBalance": "23.72469206", // crossed wallet balance
  		"crossUnPnl": "0.00000000"  // unrealized profit of crossed positions
  		"availableBalance": "23.72469206",       // available balance
  		"maxWithdrawAmount": "23.72469206",     // maximum amount for transfer out
  		"marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
  		"updateTime": 1617939110373
	}
]
```

``
GET /fapi/v2/balance (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES




## Account Information V4 (USER_DATA)


> **Response:**

```javascript

{
   "feeTier": 0, // account commisssion tier
   "canTrade": true, // if can trade
   "canDeposit": true, // if can transfer in asset
   "canWithdraw": true, // if can transfer out asset
   "updateTime": 0,
   "totalInitialMargin": "0.00000000", // total initial margin required with current mark price (useless with isolated positions), only for USDT asset
   "totalMaintMargin": "0.00000000", // total maintenance margin required, only for USDT asset
   "totalWalletBalance": "23.72469206", // total wallet balance, using BidRate/AskRate for value caculation under multi-asset mode
   "totalUnrealizedProfit": "0.00000000", // total unrealized profit in USDT
   "totalMarginBalance": "23.72469206", // total margin balance, using BidRate/AskRate for value caculation under multi-asset mode
   "totalPositionInitialMargin": "0.00000000", // initial margin required for positions with current mark price, only for USDT asset
   "totalOpenOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price, only for USDT asset
   "totalCrossWalletBalance": "23.72469206", // crossed wallet balance, using BidRate/AskRate for value caculation under multi-asset mode
   "totalCrossUnPnl": "0.00000000", // unrealized profit of crossed positions in USDT
   "availableBalance": "23.72469206", // available balance, only for USDT asset
   "maxWithdrawAmount": "23.72469206" // maximum amount for transfer out, using BidRate for value caculation under multi-asset mode
   "assets": [
       {
           "asset": "USDT", // asset name
           "walletBalance": "23.72469206", // wallet balance
           "unrealizedProfit": "0.00000000", // unrealized profit
           "marginBalance": "23.72469206", // margin balance
           "maintMargin": "0.00000000", // maintenance margin required
           "initialMargin": "0.00000000", // total initial margin required with current mark price
           "positionInitialMargin": "0.00000000", //initial margin required for positions with current mark price
           "openOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price
           "crossWalletBalance": "23.72469206", // crossed wallet balance
           "crossUnPnl": "0.00000000" // unrealized profit of crossed positions
           "availableBalance": "23.72469206", // available balance
           "maxWithdrawAmount": "23.72469206", // maximum amount for transfer out
           "marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
           "updateTime": 1625474304765 // last update time
       },
       {
           "asset": "BUSD", // asset name
           "walletBalance": "103.12345678", // wallet balance
           "unrealizedProfit": "0.00000000", // unrealized profit
           "marginBalance": "103.12345678", // margin balance
           "maintMargin": "0.00000000", // maintenance margin required
           "initialMargin": "0.00000000", // total initial margin required with current mark price
           "positionInitialMargin": "0.00000000", //initial margin required for positions with current mark price
           "openOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price
           "crossWalletBalance": "103.12345678", // crossed wallet balance
           "crossUnPnl": "0.00000000" // unrealized profit of crossed positions
           "availableBalance": "103.12345678", // available balance
           "maxWithdrawAmount": "103.12345678", // maximum amount for transfer out
           "marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
           "updateTime": 1625474304765 // last update time
       }
   ],
   "positions": [ // positions of all symbols in the market are returned
       // only "BOTH" positions will be returned with One-way mode
       // only "LONG" and "SHORT" positions will be returned with Hedge mode
       {
           "symbol": "BTCUSDT", // symbol name
           "initialMargin": "0", // initial margin required with current mark price
           "maintMargin": "0", // maintenance margin required
           "unrealizedProfit": "0.00000000", // unrealized profit
           "positionInitialMargin": "0", // initial margin required for positions with current mark price
           "openOrderInitialMargin": "0", // initial margin required for open orders with current mark price
           "leverage": "100", // current initial leverage
           "isolated": true, // if the position is isolated
           "entryPrice": "0.00000", // average entry price
           "maxNotional": "250000", // maximum available notional with current leverage
           "positionSide": "BOTH", // position side
           "positionAmt": "0", // position amount
           "updateTime": 0 // last update time
       }
   ]
}
```


``
GET /fapi/v4/account (HMAC SHA256)
``

Get current account information.

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES |







## Change Initial Leverage (TRADE)

> **Response:**

```javascript
{
 	"leverage": 21,
 	"maxNotionalValue": "1000000",
 	"symbol": "BTCUSDT"
}
```

``
POST /fapi/v1/leverage (HMAC SHA256)
``

Change user's initial leverage of specific symbol market.

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES | 
leverage | INT | YES | target initial leverage: int from 1 to 125
recvWindow | LONG | NO |
timestamp | LONG | YES |


## Change Margin Type (TRADE)

> **Response:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/marginType (HMAC SHA256)
``


**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol	 | STRING | YES	
marginType | ENUM | YES | ISOLATED, CROSSED
recvWindow | LONG | NO	
timestamp | LONG | YES


## Modify Isolated Position Margin (TRADE)

> **Response:**

```javascript
{
	"amount": 100.0,
  	"code": 200,
  	"msg": "Successfully modify position margin.",
  	"type": 1
}
```

``
POST /fapi/v1/positionMargin (HMAC SHA256)
``

**Weight:**
1


**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES	
positionSide | ENUM	| NO | Default `BOTH` for One-way Mode ; `LONG` or `SHORT` for Hedge Mode. It must be sent with Hedge Mode.
amount | DECIMAL | YES	
type | INT | YES | 1: Add position margin，2: Reduce position margin
recvWindow | LONG | NO	
timestamp | LONG | YES

* Only for isolated symbol


## Get Position Margin Change History (TRADE)

> **Response:**

```javascript
[
	{
		"amount": "23.36332311",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047897183,
	  	"type": 1,
	  	"positionSide": "BOTH"
	},
	{
		"amount": "100",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047900425,
	  	"type": 1,
	  	"positionSide": "LONG"
	}
]
```

``
GET /fapi/v1/positionMargin/history (HMAC SHA256)
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES	
type | INT	 | NO | 1: Add position margin，2: Reduce position margin
startTime | LONG | NO	
endTime | LONG | NO	
limit | INT | NO | Default: 500
recvWindow | LONG | NO	
timestamp | LONG | YES	






## Position Information V2 (USER_DATA)


> **Response:**

> For One-way position mode:

```javascript
[
  	{
  		"entryPrice": "0.00000",
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "0.00000000",	
  		"leverage": "10", 
  		"liquidationPrice": "0", 
  		"markPrice": "6679.50671178",	
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "0.000", 
  		"symbol": "BTCUSDT", 
  		"unRealizedProfit": "0.00000000", 
  		"positionSide": "BOTH",
  		"updateTime": 0
  	}
]
```

> For Hedge position mode:

```javascript
[
  	{
  		"entryPrice": "6563.66500", 
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "15517.54150468",
  		"leverage": "10",
  		"liquidationPrice": "5930.78",
  		"markPrice": "6679.50671178",	
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "20.000", 
  		"symbol": "BTCUSDT", 
  		"unRealizedProfit": "2316.83423560"
  		"positionSide": "LONG", 
  		"updateTime": 1625474304765
  	},
  	{
  		"entryPrice": "0.00000",
  		"marginType": "isolated", 
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "5413.95799991", 
  		"leverage": "10", 
  		"liquidationPrice": "7189.95", 
  		"markPrice": "6679.50671178",	
  		"maxNotionalValue": "20000000", 
  		"positionAmt": "-10.000", 
  		"symbol": "BTCUSDT",
  		"unRealizedProfit": "-1156.46711780" 
  		"positionSide": "SHORT",
  		"updateTime": 0
  	}
]
```

``
GET /fapi/v2/positionRisk (HMAC SHA256)
``

Get current position information.

**Weight:**
5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO
recvWindow | LONG | NO |
timestamp | LONG | YES |

**Note**    
Please use with user data stream `ACCOUNT_UPDATE` to meet your timeliness and accuracy needs.



## Account Trade List (USER_DATA)


> **Response:**

```javascript
[
  {
  	"buyer": false,
  	"commission": "-0.07819010",
  	"commissionAsset": "USDT",
  	"id": 698759,
  	"maker": false,
  	"orderId": 25851813,
  	"price": "7819.01",
  	"qty": "0.002",
  	"quoteQty": "15.63802",
  	"realizedPnl": "-0.91539999",
  	"side": "SELL",
  	"positionSide": "SHORT",
  	"symbol": "BTCUSDT",
  	"time": 1569514978020
  }
]
```

``
GET /fapi/v1/userTrades  (HMAC SHA256)
``

Get trades for a specific account and symbol.

**Weight:**
5 

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
startTime | LONG | NO |
endTime | LONG | NO |
fromId | LONG | NO | Trade id to fetch from. Default gets most recent trades.
limit | INT | NO | Default 500; max 1000.
recvWindow | LONG | NO |
timestamp | LONG | YES |

* If `startTime` and `endTime` are both not sent, then the last 7 days' data will be returned.
* The time between `startTime` and `endTime` cannot be longer than 7 days.
* The parameter `fromId` cannot be sent with `startTime` or `endTime`.


## Get Income History(USER_DATA)


> **Response:**

```javascript
[
	{
    	"symbol": "",					// trade symbol, if existing
    	"incomeType": "TRANSFER",	// income type
    	"income": "-0.37500000",  // income amount
    	"asset": "USDT",				// income asset
    	"info":"TRANSFER",			// extra information
    	"time": 1570608000000,		
    	"tranId":"9689322392",		// transaction id
    	"tradeId":""					// trade id, if existing
	},
	{
   		"symbol": "BTCUSDT",
    	"incomeType": "COMMISSION", 
    	"income": "-0.01000000",
    	"asset": "USDT",
    	"info":"COMMISSION",
    	"time": 1570636800000,
    	"tranId":"9689322392",
    	"tradeId":"2059192"
	}
]
```

``
GET /fapi/v1/income (HMAC SHA256)
``

**Weight:**
30

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | NO|
incomeType | STRING | NO | "TRANSFER"，"WELCOME_BONUS", "REALIZED_PNL"，"FUNDING_FEE", "COMMISSION", "INSURANCE_CLEAR", and "MARKET_MERCHANT_RETURN_REWARD"
startTime | LONG | NO | Timestamp in ms to get funding from INCLUSIVE.
endTime | LONG | NO | Timestamp in ms to get funding until INCLUSIVE.
limit | INT | NO | Default 100; max 1000
recvWindow|LONG|NO| 
timestamp|LONG|YES|

* If neither `startTime` nor `endTime` is sent, the recent 7-day data will be returned.
* If `incomeType ` is not sent, all kinds of flow will be returned
* "trandId" is unique in the same incomeType for a user


## Notional and Leverage Brackets (USER_DATA)


> **Response:**

```javascript
[
    {
        "symbol": "ETHUSDT",
        "brackets": [
            {
                "bracket": 1,   // Notional bracket
                "initialLeverage": 75,  // Max initial leverage for this bracket
                "notionalCap": 10000,  // Cap notional of this bracket
                "notionalFloor": 0,  // Notional threshold of this bracket 
                "maintMarginRatio": 0.0065, // Maintenance ratio for this bracket
                "cum":0 // Auxiliary number for quick calculation 
               
            },
        ]
    }
]
```

> **OR** (if symbol sent)

```javascript

{
    "symbol": "ETHUSDT",
    "brackets": [
        {
            "bracket": 1,
            "initialLeverage": 75,
            "notionalCap": 10000,
            "notionalFloor": 0,
            "maintMarginRatio": 0.0065,
            "cum":0
        },
    ]
}
```


``
GET /fapi/v1/leverageBracket
``


**Weight:** 1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol	| STRING | NO
recvWindow|LONG|NO| 
timestamp|LONG|YES|



## Position ADL Quantile Estimation (USER_DATA)


> **Response:**

```javascript
[
	{
		"symbol": "ETHUSDT", 
		"adlQuantile": 
			{
				// if the positions of the symbol are crossed margined in Hedge Mode, "LONG" and "SHORT" will be returned a same quantile value, and "HEDGE" will be returned instead of "BOTH".
				"LONG": 3,  
				"SHORT": 3, 
				"HEDGE": 0   // only a sign, ignore the value
			}
		},
 	{
 		"symbol": "BTCUSDT", 
 		"adlQuantile": 
 			{
 				// for positions of the symbol are in One-way Mode or isolated margined in Hedge Mode
 				"LONG": 1, 	// adl quantile for "LONG" position in hedge mode
 				"SHORT": 2, 	// adl qauntile for "SHORT" position in hedge mode
 				"BOTH": 0		// adl qunatile for position in one-way mode
 			}
 	}
 ]
```

``
GET /fapi/v1/adlQuantile
``


**Weight:** 5

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol	| STRING | NO
recvWindow|LONG|NO| 
timestamp|LONG|YES|

* Values update every 30s.

* Values 0, 1, 2, 3, 4 shows the queue position and possibility of ADL from low to high.

* For positions of the symbol are in One-way Mode or isolated margined in Hedge Mode, "LONG", "SHORT", and "BOTH" will be returned to show the positions' adl quantiles of different position sides. 

* If the positions of the symbol are crossed margined in Hedge Mode:
	* "HEDGE" as a sign will be returned instead of "BOTH"; 
	* A same value caculated on unrealized pnls on long and short sides' positions will be shown for "LONG" and "SHORT" when there are positions in both of long and short sides.



## User's Force Orders (USER_DATA)


> **Response:**

```javascript
[
  {
  	"orderId": 6071832819, 
  	"symbol": "BTCUSDT", 
  	"status": "FILLED", 
  	"clientOrderId": "autoclose-1596107620040000020", 
  	"price": "10871.09", 
  	"avgPrice": "10913.21000", 
  	"origQty": "0.001", 
  	"executedQty": "0.001", 
  	"cumQuote": "10.91321", 
  	"timeInForce": "IOC", 
  	"type": "LIMIT", 
  	"reduceOnly": false, 
  	"closePosition": false, 
  	"side": "SELL", 
  	"positionSide": "BOTH", 
  	"stopPrice": "0", 
  	"workingType": "CONTRACT_PRICE", 
  	"origType": "LIMIT", 
  	"time": 1596107620044, 
  	"updateTime": 1596107620087
  }
  {
   	"orderId": 6072734303, 
   	"symbol": "BTCUSDT", 
   	"status": "FILLED", 
   	"clientOrderId": "adl_autoclose", 
   	"price": "11023.14", 
   	"avgPrice": "10979.82000", 
   	"origQty": "0.001", 
   	"executedQty": "0.001", 
   	"cumQuote": "10.97982", 
   	"timeInForce": "GTC", 
   	"type": "LIMIT", 
   	"reduceOnly": false, 
   	"closePosition": false, 
   	"side": "BUY", 
   	"positionSide": "SHORT", 
   	"stopPrice": "0", 
   	"workingType": "CONTRACT_PRICE", 
   	"origType": "LIMIT", 
   	"time": 1596110725059, 
   	"updateTime": 1596110725071
  }
]
```


``
GET /fapi/v1/forceOrders
``


**Weight:** 20 with symbol, 50 without symbol

**Parameters:**

  Name      |  Type  | Mandatory |                         Description
------------- | ------ | --------- | -----------------------------------------------------------
symbol        | STRING | NO        |
autoCloseType | ENUM   | NO        | "LIQUIDATION" for liquidation orders, "ADL" for ADL orders.
startTime     | LONG   | NO        |
endTime       | LONG   | NO        |
limit         | INT    | NO        | Default 50; max 100.
recvWindow    | LONG   | NO        |
timestamp     | LONG   | YES       |

* If "autoCloseType" is not sent, orders with both of the types will be returned
* If "startTime" is not sent, data within 7 days before "endTime" can be queried 



## User Commission Rate (USER_DATA)

> **Response:**

```javascript
{
	"symbol": "BTCUSDT",
  	"makerCommissionRate": "0.0002",  // 0.02%
  	"takerCommissionRate": "0.0004"   // 0.04%
}
```

``
GET /fapi/v1/commissionRate (HMAC SHA256)
``

**Weight:**
20


**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
symbol | STRING | YES	
recvWindow | LONG | NO	
timestamp | LONG | YES





# User Data Streams

* The base API endpoint is: **https://fapi.asterdex.com**
* A User Data Stream `listenKey` is valid for 60 minutes after creation.
* Doing a `PUT` on a `listenKey` will extend its validity for 60 minutes.
* Doing a `DELETE` on a `listenKey` will close the stream and invalidate the `listenKey`.
* Doing a `POST` on an account with an active `listenKey` will return the currently active `listenKey` and extend its validity for 60 minutes.
* The baseurl for websocket is **wss://fstream.asterdex.com**
* User Data Streams are accessed at **/ws/\<listenKey\>**
* User data stream payloads are **not guaranteed** to be in order during heavy periods; **make sure to order your updates using E**
* A single connection to **fstream.asterdex.com** is only valid for 24 hours; expect to be disconnected at the 24 hour mark


## Start User Data Stream (USER_STREAM)


> **Response:**

```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

``
POST /fapi/v1/listenKey
``

Start a new user data stream. The stream will close after 60 minutes unless a keepalive is sent. If the account has an active `listenKey`, that `listenKey` will be returned and its validity will be extended for 60 minutes.

**Weight:**
1

**Parameters:**

None



## Keepalive User Data Stream (USER_STREAM)

> **Response:**

```javascript
{}
```

``
PUT /fapi/v1/listenKey
``

Keepalive a user data stream to prevent a time out. User data streams will close after 60 minutes. It's recommended to send a ping about every 60 minutes.

**Weight:**
1

**Parameters:**

None



## Close User Data Stream (USER_STREAM)


> **Response:**

```javascript
{}
```

``
DELETE /fapi/v1/listenKey
``

Close out a user data stream.

**Weight:**
1

**Parameters:**

None


## Event: User Data Stream Expired

> **Payload:**

```javascript
{
	'e': 'listenKeyExpired',      // event type
	'E': 1576653824250				// event time
}
```

When the `listenKey` used for the user data stream turns expired, this event will be pushed.

**Notice:**

* This event is not related to the websocket disconnection.
* This event will be received only when a valid `listenKey` in connection got expired.
* No more user data event will be updated after this event received until a new valid `listenKey` used.





## Event: Margin Call

> **Payload:**

```javascript
{
    "e":"MARGIN_CALL",    	// Event Type
    "E":1587727187525,		// Event Time
    "cw":"3.16812045",		// Cross Wallet Balance. Only pushed with crossed position margin call
    "p":[					// Position(s) of Margin Call
      {
        "s":"ETHUSDT",		// Symbol
        "ps":"LONG",		// Position Side
        "pa":"1.327",		// Position Amount
        "mt":"CROSSED",		// Margin Type
        "iw":"0",			// Isolated Wallet (if isolated position)
        "mp":"187.17127",	// Mark Price
        "up":"-1.166074",	// Unrealized PnL
        "mm":"1.614445"		// Maintenance Margin Required
      }
    ]
}  
 
```


* When the user's position risk ratio is too high, this stream will be pushed.
* This message is only used as risk guidance information and is not recommended for investment strategies.
* In the case of a highly volatile market, there may be the possibility that the user's position has been liquidated at the same time when this stream is pushed out.





## Event: Balance and Position Update


> **Payload:**

```javascript
{
  "e": "ACCOUNT_UPDATE",				// Event Type
  "E": 1564745798939,            		// Event Time
  "T": 1564745798938 ,           		// Transaction
  "a":                          		// Update Data
    {
      "m":"ORDER",						// Event reason type
      "B":[                     		// Balances
        {
          "a":"USDT",           		// Asset
          "wb":"122624.12345678",    	// Wallet Balance
          "cw":"100.12345678",			// Cross Wallet Balance
          "bc":"50.12345678"			// Balance Change except PnL and Commission
        },
        {
          "a":"BUSD",           
          "wb":"1.00000000",
          "cw":"0.00000000",         
          "bc":"-49.12345678"
        }
      ],
      "P":[
        {
          "s":"BTCUSDT",          	// Symbol
          "pa":"0",               	// Position Amount
          "ep":"0.00000",            // Entry Price
          "cr":"200",             	// (Pre-fee) Accumulated Realized
          "up":"0",						// Unrealized PnL
          "mt":"isolated",				// Margin Type
          "iw":"0.00000000",			// Isolated Wallet (if isolated position)
          "ps":"BOTH"					// Position Side
        }，
        {
        	"s":"BTCUSDT",
        	"pa":"20",
        	"ep":"6563.66500",
        	"cr":"0",
        	"up":"2850.21200",
        	"mt":"isolated",
        	"iw":"13200.70726908",
        	"ps":"LONG"
      	 },
        {
        	"s":"BTCUSDT",
        	"pa":"-10",
        	"ep":"6563.86000",
        	"cr":"-45.04000000",
        	"up":"-1423.15600",
        	"mt":"isolated",
        	"iw":"6570.42511771",
        	"ps":"SHORT"
        }
      ]
    }
}
```

Event type is `ACCOUNT_UPDATE`.   

* When balance or position get updated, this event will be pushed.
	* `ACCOUNT_UPDATE` will be pushed only when update happens on user's account, including changes on balances, positions, or margin type.
	* Unfilled orders or cancelled orders will not make the event `ACCOUNT_UPDATE` pushed, since there's no change on positions.
	* Only positions of symbols with non-zero isolatd wallet or non-zero position amount will be pushed in the "position" part of the event `ACCOUNT_UPDATE` when any position changes.

* When "FUNDING FEE" changes to the user's balance, the event will be pushed with the brief message:
	* When "FUNDING FEE" occurs in a **crossed position**, `ACCOUNT_UPDATE` will be pushed with only the balance `B`(including the "FUNDING FEE" asset only), without any position `P` message. 
	* When "FUNDING FEE" occurs in an **isolated position**, `ACCOUNT_UPDATE` will be pushed with only the balance `B`(including the "FUNDING FEE" asset only) and the relative position message `P`( including the isolated position on which the "FUNDING FEE" occurs only, without any other position message).  

* The field "m" represents the reason type for the event and may shows the following possible types:
	* DEPOSIT
	* WITHDRAW
	* ORDER
	* FUNDING_FEE
	* WITHDRAW_REJECT
	* ADJUSTMENT
	* INSURANCE_CLEAR
	* ADMIN_DEPOSIT
	* ADMIN_WITHDRAW
	* MARGIN_TRANSFER
	* MARGIN_TYPE_CHANGE
	* ASSET_TRANSFER
	* OPTIONS_PREMIUM_FEE
	* OPTIONS_SETTLE_PROFIT
	* AUTO_EXCHANGE

* The field "bc" represents the balance change except for PnL and commission.

## Event: Order Update


> **Payload:**

```javascript
{
  
  "e":"ORDER_TRADE_UPDATE",		// Event Type
  "E":1568879465651,			// Event Time
  "T":1568879465650,			// Transaction Time
  "o":{								
    "s":"BTCUSDT",				// Symbol
    "c":"TEST",					// Client Order Id
      // special client order id:
      // starts with "autoclose-": liquidation order
      // "adl_autoclose": ADL auto close order
    "S":"SELL",					// Side
    "o":"TRAILING_STOP_MARKET",	// Order Type
    "f":"GTC",					// Time in Force
    "q":"0.001",				// Original Quantity
    "p":"0",					// Original Price
    "ap":"0",					// Average Price
    "sp":"7103.04",				// Stop Price. Please ignore with TRAILING_STOP_MARKET order
    "x":"NEW",					// Execution Type
    "X":"NEW",					// Order Status
    "i":8886774,				// Order Id
    "l":"0",					// Order Last Filled Quantity
    "z":"0",					// Order Filled Accumulated Quantity
    "L":"0",					// Last Filled Price
    "N":"USDT",            	// Commission Asset, will not push if no commission
    "n":"0",               	// Commission, will not push if no commission
    "T":1568879465651,			// Order Trade Time
    "t":0,			        	// Trade Id
    "b":"0",			    	// Bids Notional
    "a":"9.91",					// Ask Notional
    "m":false,					// Is this trade the maker side?
    "R":false,					// Is this reduce only
    "wt":"CONTRACT_PRICE", 		// Stop Price Working Type
    "ot":"TRAILING_STOP_MARKET",	// Original Order Type
    "ps":"LONG",						// Position Side
    "cp":false,						// If Close-All, pushed with conditional order
    "AP":"7476.89",				// Activation Price, only puhed with TRAILING_STOP_MARKET order
    "cr":"5.0",					// Callback Rate, only puhed with TRAILING_STOP_MARKET order
    "rp":"0"							// Realized Profit of the trade
  }
  
}
```


When new order created, order status changed will push such event.
event type is `ORDER_TRADE_UPDATE`.





**Side**

* BUY 
* SELL 

**Order Type**

* MARKET 
* LIMIT
* STOP
* TAKE_PROFIT
* LIQUIDATION

**Execution Type**

* NEW
* CANCELED
* CALCULATED		 - Liquidation Execution
* EXPIRED
* TRADE

**Order Status**

* NEW
* PARTIALLY_FILLED
* FILLED
* CANCELED
* EXPIRED
* NEW_INSURANCE     - Liquidation with Insurance Fund
* NEW_ADL				- Counterparty Liquidation`

**Time in force**

* GTC
* IOC
* FOK
* GTX
* HIDDEN

**Working Type**

* MARK_PRICE
* CONTRACT_PRICE 



## Event: Account Configuration Update previous Leverage Update

> **Payload:**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
    "E":1611646737479,		           // Event Time
    "T":1611646737476,		           // Transaction Time
    "ac":{								
    "s":"BTCUSDT",					   // symbol
    "l":25						       // leverage
     
    }
}  
 
```

> **Or**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // Event Type
    "E":1611646737479,		           // Event Time
    "T":1611646737476,		           // Transaction Time
    "ai":{							   // User's Account Configuration
    "j":true						   // Multi-Assets Mode
    }
}  
```

When the account configuration is changed, the event type will be pushed as `ACCOUNT_CONFIG_UPDATE`

When the leverage of a trade pair changes, the payload will contain the object `ac` to represent the account configuration of the trade pair, where `s` represents the specific trade pair and `l` represents the leverage

When the user Multi-Assets margin mode changes the payload will contain the object `ai` representing the user account configuration, where `j` represents the user Multi-Assets margin mode



# Error Codes

> Here is the error JSON payload:
 
```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

Errors consist of two parts: an error code and a message.    
Codes are universal,but messages can vary. 



## 10xx - General Server or Network issues
> -1000 UNKNOWN
 * An unknown error occured while processing the request.

> -1001 DISCONNECTED
 * Internal error; unable to process your request. Please try again.

> -1002 UNAUTHORIZED
 * You are not authorized to execute this request.

> -1003 TOO_MANY_REQUESTS
 * Too many requests queued.
 * Too many requests; please use the websocket for live updates.
 * Too many requests; current limit is %s requests per minute. Please use the websocket for live updates to avoid polling the API.
 * Way too many requests; IP banned until %s. Please use the websocket for live updates to avoid bans.
 
> -1004 DUPLICATE_IP
 * This IP is already on the white list

> -1005 NO_SUCH_IP
 * No such IP has been white listed
 
> -1006 UNEXPECTED_RESP
 * An unexpected response was received from the message bus. Execution status unknown.

> -1007 TIMEOUT
 * Timeout waiting for response from backend server. Send status unknown; execution status unknown.

> -1010 ERROR_MSG_RECEIVED
 * ERROR_MSG_RECEIVED.  
 
> -1011 NON_WHITE_LIST
 * This IP cannot access this route. 
 
> -1013 INVALID_MESSAGE
* INVALID_MESSAGE.

> -1014 UNKNOWN_ORDER_COMPOSITION
 * Unsupported order combination.

> -1015 TOO_MANY_ORDERS
 * Too many new orders.
 * Too many new orders; current limit is %s orders per %s.

> -1016 SERVICE_SHUTTING_DOWN
 * This service is no longer available.

> -1020 UNSUPPORTED_OPERATION
 * This operation is not supported.

> -1021 INVALID_TIMESTAMP
 * Timestamp for this request is outside of the recvWindow.
 * Timestamp for this request was 1000ms ahead of the server's time.

> -1022 INVALID_SIGNATURE
 * Signature for this request is not valid.

> -1023 START_TIME_GREATER_THAN_END_TIME
 * Start time is greater than end time.


## 11xx - Request issues
> -1100 ILLEGAL_CHARS
 * Illegal characters found in a parameter.
 * Illegal characters found in parameter '%s'; legal range is '%s'.

> -1101 TOO_MANY_PARAMETERS
 * Too many parameters sent for this endpoint.
 * Too many parameters; expected '%s' and received '%s'.
 * Duplicate values for a parameter detected.

> -1102 MANDATORY_PARAM_EMPTY_OR_MALFORMED
 * A mandatory parameter was not sent, was empty/null, or malformed.
 * Mandatory parameter '%s' was not sent, was empty/null, or malformed.
 * Param '%s' or '%s' must be sent, but both were empty/null!

> -1103 UNKNOWN_PARAM
 * An unknown parameter was sent.

> -1104 UNREAD_PARAMETERS
 * Not all sent parameters were read.
 * Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.

> -1105 PARAM_EMPTY
 * A parameter was empty.
 * Parameter '%s' was empty.

> -1106 PARAM_NOT_REQUIRED
 * A parameter was sent when not required.
 * Parameter '%s' sent when not required.

> -1108 BAD_ASSET
 * Invalid asset.

> -1109 BAD_ACCOUNT
 * Invalid account.

> -1110 BAD_INSTRUMENT_TYPE
 * Invalid symbolType.
 
> -1111 BAD_PRECISION
 * Precision is over the maximum defined for this asset.

> -1112 NO_DEPTH
 * No orders on book for symbol.
 
> -1113 WITHDRAW_NOT_NEGATIVE
 * Withdrawal amount must be negative.
 
> -1114 TIF_NOT_REQUIRED
 * TimeInForce parameter sent when not required.

> -1115 INVALID_TIF
 * Invalid timeInForce.

> -1116 INVALID_ORDER_TYPE
 * Invalid orderType.

> -1117 INVALID_SIDE
 * Invalid side.

> -1118 EMPTY_NEW_CL_ORD_ID
 * New client order ID was empty.

> -1119 EMPTY_ORG_CL_ORD_ID
 * Original client order ID was empty.

> -1120 BAD_INTERVAL
 * Invalid interval.

> -1121 BAD_SYMBOL
 * Invalid symbol.

> -1125 INVALID_LISTEN_KEY
 * This listenKey does not exist.

> -1127 MORE_THAN_XX_HOURS
 * Lookup interval is too big.
 * More than %s hours between startTime and endTime.

> -1128 OPTIONAL_PARAMS_BAD_COMBO
 * Combination of optional parameters invalid.

> -1130 INVALID_PARAMETER
 * Invalid data sent for a parameter.
 * Data sent for parameter '%s' is not valid.

> -1136 INVALID_NEW_ORDER_RESP_TYPE
 * Invalid newOrderRespType.


## 20xx - Processing Issues

> -2010 NEW_ORDER_REJECTED
 * NEW_ORDER_REJECTED

> -2011 CANCEL_REJECTED
 * CANCEL_REJECTED

> -2013 NO_SUCH_ORDER
 * Order does not exist.

> -2014 BAD_API_KEY_FMT
 * API-key format invalid.

> -2015 REJECTED_MBX_KEY
 * Invalid API-key, IP, or permissions for action.

> -2016 NO_TRADING_WINDOW
 * No trading window could be found for the symbol. Try ticker/24hrs instead.

> -2018 BALANCE_NOT_SUFFICIENT
 * Balance is insufficient.

> -2019 MARGIN_NOT_SUFFICIEN
 * Margin is insufficient.

> -2020 UNABLE_TO_FILL
 * Unable to fill.

> -2021 ORDER_WOULD_IMMEDIATELY_TRIGGER
 * Order would immediately trigger.

> -2022 REDUCE_ONLY_REJECT
 * ReduceOnly Order is rejected.

> -2023 USER_IN_LIQUIDATION
 * User in liquidation mode now.

> -2024 POSITION_NOT_SUFFICIENT
 * Position is not sufficient.

> -2025 MAX_OPEN_ORDER_EXCEEDED
 * Reach max open order limit.

> -2026 REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED
 * This OrderType is not supported when reduceOnly.

> -2027 MAX_LEVERAGE_RATIO
 * Exceeded the maximum allowable position at current leverage.


> -2028 MIN_LEVERAGE_RATIO
 * Leverage is smaller than permitted: insufficient margin balance.


## 40xx - Filters and other Issues
> -4000 INVALID_ORDER_STATUS
 * Invalid order status.

> -4001 PRICE_LESS_THAN_ZERO
 * Price less than 0.

> -4002 PRICE_GREATER_THAN_MAX_PRICE
 * Price greater than max price.
 
> -4003 QTY_LESS_THAN_ZERO
 * Quantity less than zero.

> -4004 QTY_LESS_THAN_MIN_QTY
 * Quantity less than min quantity.
 
> -4005 QTY_GREATER_THAN_MAX_QTY
 * Quantity greater than max quantity. 

> -4006 STOP_PRICE_LESS_THAN_ZERO
 * Stop price less than zero. 
 
> -4007 STOP_PRICE_GREATER_THAN_MAX_PRICE
 * Stop price greater than max price. 

> -4008 TICK_SIZE_LESS_THAN_ZERO
 * Tick size less than zero.

> -4009 MAX_PRICE_LESS_THAN_MIN_PRICE
 * Max price less than min price.

> -4010 MAX_QTY_LESS_THAN_MIN_QTY
 * Max qty less than min qty.

> -4011 STEP_SIZE_LESS_THAN_ZERO
 * Step size less than zero.

> -4012 MAX_NUM_ORDERS_LESS_THAN_ZERO
 * Max mum orders less than zero.

> -4013 PRICE_LESS_THAN_MIN_PRICE
 * Price less than min price.

> -4014 PRICE_NOT_INCREASED_BY_TICK_SIZE
 * Price not increased by tick size.
 
> -4015 INVALID_CL_ORD_ID_LEN
 * Client order id is not valid.
 * Client order id length should not be more than 36 chars

> -4016 PRICE_HIGHTER_THAN_MULTIPLIER_UP
 * Price is higher than mark price multiplier cap.

> -4017 MULTIPLIER_UP_LESS_THAN_ZERO
 * Multiplier up less than zero.

> -4018 MULTIPLIER_DOWN_LESS_THAN_ZERO
 * Multiplier down less than zero.

> -4019 COMPOSITE_SCALE_OVERFLOW
 * Composite scale too large.

> -4020 TARGET_STRATEGY_INVALID
 * Target strategy invalid for orderType '%s',reduceOnly '%b'.

> -4021 INVALID_DEPTH_LIMIT
 * Invalid depth limit.
 * '%s' is not valid depth limit.

> -4022 WRONG_MARKET_STATUS
 * market status sent is not valid.
 
> -4023 QTY_NOT_INCREASED_BY_STEP_SIZE
 * Qty not increased by step size.

> -4024 PRICE_LOWER_THAN_MULTIPLIER_DOWN
 * Price is lower than mark price multiplier floor.

> -4025 MULTIPLIER_DECIMAL_LESS_THAN_ZERO
 * Multiplier decimal less than zero.

> -4026 COMMISSION_INVALID
 * Commission invalid.
 * `%s` less than zero.
 * `%s` absolute value greater than `%s`

> -4027 INVALID_ACCOUNT_TYPE
 * Invalid account type.

> -4028 INVALID_LEVERAGE
 * Invalid leverage
 * Leverage `%s` is not valid
 * Leverage `%s` already exist with `%s`

> -4029 INVALID_TICK_SIZE_PRECISION
 * Tick size precision is invalid.

> -4030 INVALID_STEP_SIZE_PRECISION
 * Step size precision is invalid.

> -4031 INVALID_WORKING_TYPE
 * Invalid parameter working type
 * Invalid parameter working type: `%s`

> -4032 EXCEED_MAX_CANCEL_ORDER_SIZE
 * Exceed maximum cancel order size.
 * Invalid parameter working type: `%s`

> -4033 INSURANCE_ACCOUNT_NOT_FOUND
 * Insurance account not found.

> -4044 INVALID_BALANCE_TYPE
 * Balance Type is invalid.

> -4045 MAX_STOP_ORDER_EXCEEDED
 * Reach max stop order limit.

> -4046 NO_NEED_TO_CHANGE_MARGIN_TYPE
 * No need to change margin type.

> -4047 THERE_EXISTS_OPEN_ORDERS
 * Margin type cannot be changed if there exists open orders.

> -4048 THERE_EXISTS_QUANTITY
 * Margin type cannot be changed if there exists position.

> -4049 ADD_ISOLATED_MARGIN_REJECT
 * Add margin only support for isolated position.

> -4050 CROSS_BALANCE_INSUFFICIENT
 * Cross balance insufficient.

> -4051 ISOLATED_BALANCE_INSUFFICIENT
 * Isolated balance insufficient.

> -4052 NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN
 * No need to change auto add margin.

> -4053 AUTO_ADD_CROSSED_MARGIN_REJECT
 * Auto add margin only support for isolated position.

> -4054 ADD_ISOLATED_MARGIN_NO_POSITION_REJECT
 * Cannot add position margin: position is 0.

> -4055 AMOUNT_MUST_BE_POSITIVE
 * Amount must be positive.

> -4056 INVALID_API_KEY_TYPE
 * Invalid api key type.

> -4057 INVALID_RSA_PUBLIC_KEY
 * Invalid api public key

> -4058 MAX_PRICE_TOO_LARGE
 * maxPrice and priceDecimal too large,please check.

> -4059 NO_NEED_TO_CHANGE_POSITION_SIDE
 * No need to change position side.

> -4060 INVALID_POSITION_SIDE
 * Invalid position side.

> -4061 POSITION_SIDE_NOT_MATCH
 * Order's position side does not match user's setting.

> -4062 REDUCE_ONLY_CONFLICT
 * Invalid or improper reduceOnly value.

> -4063 INVALID_OPTIONS_REQUEST_TYPE
 * Invalid options request type

> -4064 INVALID_OPTIONS_TIME_FRAME
 * Invalid options time frame

> -4065 INVALID_OPTIONS_AMOUNT
 * Invalid options amount

> -4066 INVALID_OPTIONS_EVENT_TYPE
 * Invalid options event type

> -4067 POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS
 * Position side cannot be changed if there exists open orders.

> -4068 POSITION_SIDE_CHANGE_EXISTS_QUANTITY
 * Position side cannot be changed if there exists position.

> -4069 INVALID_OPTIONS_PREMIUM_FEE
 * Invalid options premium fee

> -4070 INVALID_CL_OPTIONS_ID_LEN
 * Client options id is not valid.
 * Client options id length should be less than 32 chars

> -4071 INVALID_OPTIONS_DIRECTION
 * Invalid options direction

> -4072 OPTIONS_PREMIUM_NOT_UPDATE
 * premium fee is not updated, reject order

> -4073 OPTIONS_PREMIUM_INPUT_LESS_THAN_ZERO
 * input premium fee is less than 0, reject order

> -4074 OPTIONS_AMOUNT_BIGGER_THAN_UPPER
 * Order amount is bigger than upper boundary or less than 0, reject order

> -4075 OPTIONS_PREMIUM_OUTPUT_ZERO
 * output premium fee is less than 0, reject order

> -4076 OPTIONS_PREMIUM_TOO_DIFF
 * original fee is too much higher than last fee

> -4077 OPTIONS_PREMIUM_REACH_LIMIT
 * place order amount has reached to limit, reject order

> -4078 OPTIONS_COMMON_ERROR
 * options internal error

> -4079 INVALID_OPTIONS_ID
 * invalid options id
 * invalid options id: %s
 * duplicate options id %d for user %d

> -4080 OPTIONS_USER_NOT_FOUND
 * user not found
 * user not found with id: %s

> -4081 OPTIONS_NOT_FOUND
 * options not found
 * options not found with id: %s

> -4082 INVALID_BATCH_PLACE_ORDER_SIZE
 * Invalid number of batch place orders.
 * Invalid number of batch place orders: %s

> -4083 PLACE_BATCH_ORDERS_FAIL
 * Fail to place batch orders.

> -4084 UPCOMING_METHOD
 * Method is not allowed currently. Upcoming soon.

> -4085 INVALID_NOTIONAL_LIMIT_COEF
 * Invalid notional limit coefficient

> -4086 INVALID_PRICE_SPREAD_THRESHOLD
 * Invalid price spread threshold

> -4087 REDUCE_ONLY_ORDER_PERMISSION
 * User can only place reduce only order

> -4088 NO_PLACE_ORDER_PERMISSION
 * User can not place order currently

> -4104 INVALID_CONTRACT_TYPE
 * Invalid contract type

> -4114 INVALID_CLIENT_TRAN_ID_LEN
 * clientTranId  is not valid
 * Client tran id length should be less than 64 chars

> -4115 DUPLICATED_CLIENT_TRAN_ID
 * clientTranId  is duplicated
 * Client tran id should be unique within 7 days

> -4118 REDUCE_ONLY_MARGIN_CHECK_FAILED
 * ReduceOnly Order Failed. Please check your existing position and open orders
 
> -4131 MARKET_ORDER_REJECT
 * The counterparty's best price does not meet the PERCENT_PRICE filter limit

> -4135 INVALID_ACTIVATION_PRICE
 * Invalid activation price

> -4137 QUANTITY_EXISTS_WITH_CLOSE_POSITION
 * Quantity must be zero with closePosition equals true

> -4138 REDUCE_ONLY_MUST_BE_TRUE
 * Reduce only must be true with closePosition equals true

> -4139 ORDER_TYPE_CANNOT_BE_MKT
 * Order type can not be market if it's unable to cancel

> -4140 INVALID_OPENING_POSITION_STATUS
 * Invalid symbol status for opening position

> -4141 SYMBOL_ALREADY_CLOSED
 * Symbol is closed

> -4142 STRATEGY_INVALID_TRIGGER_PRICE
 * REJECT: take profit or stop order will be triggered immediately

> -4144 INVALID_PAIR
 * Invalid pair

> -4161 ISOLATED_LEVERAGE_REJECT_WITH_POSITION
 * Leverage reduction is not supported in Isolated Margin Mode with open positions

> -4164 MIN_NOTIONAL
 * Order's notional must be no smaller than 5.0 (unless you choose reduce only)
 * Order's notional must be no smaller than %s (unless you choose reduce only)

> -4165 INVALID_TIME_INTERVAL
 * Invalid time interval
 * Maximum time interval is %s days

> -4183 PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP
 * Price is higher than stop price multiplier cap.
 * Limit price can't be higher than %s.

> -4184 PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN
 * Price is lower than stop price multiplier floor.
 * Limit price can't be lower than %s.


---

# aster-finance-futures-api_CN.md

- [基本信息](#基本信息)
	- [Rest 基本信息](#rest-基本信息)
		- [HTTP 返回代码](#http-返回代码)
		- [接口错误代码](#接口错误代码)
		- [接口的基本信息](#接口的基本信息)
	- [访问限制](#访问限制)
		- [IP 访问限制](#ip-访问限制)
		- [下单频率限制](#下单频率限制)
	- [接口鉴权类型](#接口鉴权类型)
	- [需要签名的接口 (TRADE 与 USER_DATA)](#需要签名的接口-trade-与-user_data)
		- [时间同步安全](#时间同步安全)
		- [POST /fapi/v1/order 的示例](#post-fapiv1order-的示例)
		- [示例 1: 所有参数通过 query string 发送](#示例-1-所有参数通过-query-string-发送)
		- [示例 2: 所有参数通过 request body 发送](#示例-2-所有参数通过-request-body-发送)
		- [示例 3: 混合使用 query string 与 request body](#示例-3-混合使用-query-string-与-request-body)
	- [公开API参数](#公开api参数)
		- [术语解释](#术语解释)
		- [枚举定义](#枚举定义)
	- [过滤器](#过滤器)
		- [交易对过滤器](#交易对过滤器)
			- [PRICE_FILTER 价格过滤器](#price_filter-价格过滤器)
			- [LOT_SIZE 订单尺寸](#lot_size-订单尺寸)
			- [MARKET_LOT_SIZE 市价订单尺寸](#market_lot_size-市价订单尺寸)
			- [MAX_NUM_ORDERS 最多订单数](#max_num_orders-最多订单数)
			- [MAX_NUM_ALGO_ORDERS 最多条件订单数](#max_num_algo_orders-最多条件订单数)
			- [PERCENT_PRICE 价格振幅过滤器](#percent_price-价格振幅过滤器)
			- [MIN_NOTIONAL 最小名义价值](#min_notional-最小名义价值)
- [行情接口](#行情接口)
	- [测试服务器连通性 PING](#测试服务器连通性-ping)
	- [获取服务器时间](#获取服务器时间)
	- [获取交易规则和交易对](#获取交易规则和交易对)
	- [深度信息](#深度信息)
	- [近期成交](#近期成交)
	- [查询历史成交(MARKET_DATA)](#查询历史成交market_data)
	- [近期成交(归集)](#近期成交归集)
	- [K线数据](#k线数据)
	- [价格指数K线数据](#价格指数k线数据)
	- [标记价格K线数据](#标记价格k线数据)
	- [最新标记价格和资金费率](#最新标记价格和资金费率)
	- [查询资金费率历史](#查询资金费率历史)
    - [查询资金费率配置](#查询资金费率配置)
	- [24hr价格变动情况](#24hr价格变动情况)
	- [最新价格](#最新价格)
	- [当前最优挂单](#当前最优挂单)
- [Websocket 行情推送](#websocket-行情推送)
	- [实时订阅/取消数据流](#实时订阅取消数据流)
		- [订阅一个信息流](#订阅一个信息流)
		- [取消订阅一个信息流](#取消订阅一个信息流)
		- [已订阅信息流](#已订阅信息流)
		- [设定属性](#设定属性)
		- [检索属性](#检索属性)
		- [错误信息](#错误信息)
	- [最新合约价格](#最新合约价格)
	- [归集交易](#归集交易)
	- [最新标记价格](#最新标记价格)
	- [全市场最新标记价格](#全市场最新标记价格)
	- [K线](#k线)
	- [按Symbol的精简Ticker](#按symbol的精简ticker)
	- [全市场的精简Ticker](#全市场的精简ticker)
	- [按Symbol的完整Ticker](#按symbol的完整ticker)
	- [全市场的完整Ticker](#全市场的完整ticker)
	- [按Symbol的最优挂单信息](#按symbol的最优挂单信息)
	- [全市场最优挂单信息](#全市场最优挂单信息)
	- [有限档深度信息](#有限档深度信息)
	- [增量深度信息](#增量深度信息)
	- [如何正确在本地维护一个orderbook副本](#如何正确在本地维护一个orderbook副本)
- [账户和交易接口](#账户和交易接口)
	- [更改持仓模式(TRADE)](#更改持仓模式trade)
	- [查询持仓模式(USER_DATA)](#查询持仓模式user_data)
	- [更改联合保证金模式(TRADE)](#更改联合保证金模式trade)
	- [查询联合保证金模式(USER_DATA)](#查询联合保证金模式user_data)
	- [下单 (TRADE)](#下单-trade)
	- [测试下单接口 (TRADE)](#测试下单接口-trade)
	- [批量下单 (TRADE)](#批量下单-trade)
	- [期货现货互转 (TRADE)](#期货现货互转-trade)
	- [查询订单 (USER_DATA)](#查询订单-user_data)
	- [撤销订单 (TRADE)](#撤销订单-trade)
	- [撤销全部订单 (TRADE)](#撤销全部订单-trade)
	- [批量撤销订单 (TRADE)](#批量撤销订单-trade)
	- [倒计时撤销所有订单 (TRADE)](#倒计时撤销所有订单-trade)
	- [查询当前挂单 (USER_DATA)](#查询当前挂单-user_data)
	- [查看当前全部挂单 (USER_DATA)](#查看当前全部挂单-user_data)
	- [查询所有订单(包括历史订单) (USER_DATA)](#查询所有订单包括历史订单-user_data)
	- [账户余额V2 (USER_DATA)](#账户余额v2-user_data)
	- [账户信息V2 (USER_DATA)](#账户信息v2-user_data)
	- [调整开仓杠杆 (TRADE)](#调整开仓杠杆-trade)
	- [变换逐全仓模式 (TRADE)](#变换逐全仓模式-trade)
	- [调整逐仓保证金 (TRADE)](#调整逐仓保证金-trade)
	- [逐仓保证金变动历史 (TRADE)](#逐仓保证金变动历史-trade)
	- [用户持仓风险V2 (USER_DATA)](#用户持仓风险v2-user_data)
	- [账户成交历史 (USER_DATA)](#账户成交历史-user_data)
	- [获取账户损益资金流水(USER_DATA)](#获取账户损益资金流水user_data)
	- [杠杆分层标准 (USER_DATA)](#杠杆分层标准-user_data)
	- [持仓ADL队列估算 (USER_DATA)](#持仓adl队列估算-user_data)
	- [用户强平单历史 (USER_DATA)](#用户强平单历史-user_data)
	- [用户手续费率 (USER_DATA)](#用户手续费率-user_data)
- [Websocket 账户信息推送](#websocket-账户信息推送)
	- [生成listenKey (USER_STREAM)](#生成listenkey-user_stream)
	- [延长listenKey有效期 (USER_STREAM)](#延长listenkey有效期-user_stream)
	- [关闭listenKey (USER_STREAM)](#关闭listenkey-user_stream)
	- [listenKey 过期推送](#listenkey-过期推送)
	- [追加保证金通知](#追加保证金通知)
	- [Balance和Position更新推送](#balance和position更新推送)
	- [订单/交易 更新推送](#订单交易-更新推送)
	- [杠杆倍数等账户配置 更新推送](#杠杆倍数等账户配置-更新推送)
- [错误代码](#错误代码)
	- [10xx - 常规服务器或网络问题](#10xx---常规服务器或网络问题)
	- [11xx - Request issues](#11xx---request-issues)
	- [20xx - Processing Issues](#20xx---processing-issues)
	- [40xx - Filters and other Issues](#40xx---filters-and-other-issues)

# 基本信息


## Rest 基本信息

* 接口可能需要用户的 API Key，如何创建API-KEY请参考[这里](https://www.asterdex.com/)
* 本篇列出REST接口的baseurl **https://fapi.asterdex.com**
* 所有接口的响应都是JSON格式
* 响应中如有数组，数组元素以时间升序排列，越早的数据越提前。
* 所有时间、时间戳均为UNIX时间，单位为毫秒
* 所有数据类型采用JAVA的数据类型定义

### HTTP 返回代码
* HTTP `4XX` 错误码用于指示错误的请求内容、行为、格式。
* HTTP `403` 错误码表示违反WAF限制(Web应用程序防火墙)。
* HTTP `429` 错误码表示警告访问频次超限，即将被封IP
* HTTP `418` 表示收到429后继续访问，于是被封了。
* HTTP `5XX` 错误码用于指示Aster Finance服务侧的问题。    
* HTTP `503` 表示API服务端已经向业务核心提交了请求但未能获取响应，特别需要注意的是其不代表请求失败，而是未知。很可能已经得到了执行，也有可能执行失败，需要做进一步确认。


### 接口错误代码
* 每个接口都有可能抛出异常

> 异常响应格式如下：

```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

* 具体的错误码及其解释在[错误代码](#错误代码)

### 接口的基本信息
* `GET`方法的接口, 参数必须在`query string`中发送.
* `POST`, `PUT`, 和 `DELETE` 方法的接口, 参数可以在 `query string`中发送，也可以在 `request body`中发送(content type `application/x-www-form-urlencoded`)。允许混合这两种方式发送参数。但如果同一个参数名在query string和request body中都有，query string中的会被优先采用。
* 对参数的顺序不做要求。

## 访问限制
* 在 `/fapi/v1/exchangeInfo`接口中`rateLimits`数组里包含有REST接口(不限于本篇的REST接口)的访问限制。包括带权重的访问频次限制、下单速率限制。本篇`枚举定义`章节有限制类型的进一步说明。
* 违反上述任何一个访问限制都会收到HTTP 429，这是一个警告.

<aside class="notice">
请注意，若用户被认定利用频繁挂撤单且故意低效交易意图发起攻击行为，Aster Finance有权视具体情况进一步加强对其访问限制。
</aside>


### IP 访问限制
* 每个请求将包含一个`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)`的头，其中包含当前IP所有请求的已使用权重。
* 每个路由都有一个"权重"，该权重确定每个接口计数的请求数。较重的接口和对多个交易对进行操作的接口将具有较重的"权重"。
* 收到429时，您有责任作为API退回而不向其发送更多的请求。
* **如果屡次违反速率限制和/或在收到429后未能退回，将导致API的IP被禁(http状态418)。**
* 频繁违反限制，封禁时间会逐渐延长 ，**对于重复违反者，将会被封从2分钟到3天**。
* **访问限制是基于IP的，而不是API Key**

<aside class="notice">
强烈建议您尽可能多地使用websocket消息获取相应数据,既可以保障消息的及时性，也可以减少请求带来的访问限制压力。
</aside>


### 下单频率限制
* 每个下单请求回报将包含一个`X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)`的头，其中包含当前账户已用的下单限制数量。
* 被拒绝或不成功的下单并不保证回报中包含以上头内容。
* **下单频率限制是基于每个账户计数的。**

## 接口鉴权类型
* 每个接口都有自己的鉴权类型，鉴权类型决定了访问时应当进行何种鉴权
* 如果需要 API-key，应当在HTTP头中以`X-MBX-APIKEY`字段传递
* API-key 与 API-secret 是大小写敏感的
* 可以在网页用户中心修改API-key 所具有的权限，例如读取账户信息、发送交易指令、发送提现指令

鉴权类型 | 描述
------------ | ------------
NONE | 不需要鉴权的接口
TRADE | 需要有效的API-KEY和签名
USER_DATA | 需要有效的API-KEY和签名
USER_STREAM | 需要有效的API-KEY
MARKET_DATA | 需要有效的API-KEY


## 需要签名的接口 (TRADE 与 USER_DATA)
* 调用这些接口时，除了接口本身所需的参数外，还需要传递`signature`即签名参数。
* 签名使用`HMAC SHA256`算法. API-KEY所对应的API-Secret作为 `HMAC SHA256` 的密钥，其他所有参数作为`HMAC SHA256`的操作对象，得到的输出即为签名。
* 签名大小写不敏感。
* 当同时使用query string和request body时，`HMAC SHA256`的输入query string在前，request body在后

### 时间同步安全
* 签名接口均需要传递`timestamp`参数, 其值应当是请求发送时刻的unix时间戳(毫秒)
* 服务器收到请求时会判断请求中的时间戳，如果是5000毫秒之前发出的，则请求会被认为无效。这个时间窗口值可以通过发送可选参数`recvWindow`来自定义。
* 另外，如果服务器计算得出客户端时间戳在服务器时间的‘未来’一秒以上，也会拒绝请求。

> 逻辑伪代码：
  
  ```javascript
  if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow) {
    // process request
  } else {
    // reject request
  }
  ```

**关于交易时效性** 
互联网状况并不100%可靠，不可完全依赖,因此你的程序本地到服务器的时延会有抖动.
这是我们设置`recvWindow`的目的所在，如果你从事高频交易，对交易时效性有较高的要求，可以灵活设置recvWindow以达到你的要求。

<aside class="notice">
不推荐使用5秒以上的recvWindow
</aside>

### POST /fapi/v1/order 的示例

以下是在linux bash环境下使用 echo openssl 和curl工具实现的一个调用接口下单的示例
apikey、secret仅供示范

Key | Value
------------ | ------------
apiKey | dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83
secretKey | 2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9


参数 | 取值
------------ | ------------
symbol | BTCUSDT
side | BUY
type | LIMIT
timeInForce | GTC
quantity | 1
price | 9000
recvWindow | 5000
timestamp | 1591702613943


### 示例 1: 所有参数通过 query string 发送

> **示例1:**

> **HMAC SHA256 签名:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```


> **curl 调用:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi.asterdex.com/fapi/v1/order?symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943&signature= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```

* **queryString:** 

	symbol=BTCUSDT    
	&side=BUY   
	&type=LIMIT   
	&timeInForce=GTC   
	&quantity=1   
	&price=0.1   
	&recvWindow=5000   
	&timestamp=1499827319559


### 示例 2: 所有参数通过 request body 发送

> **示例2:**

> **HMAC SHA256 签名:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```


> **curl 调用:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi.asterdex.com/fapi/v1/order' -d 'symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943&signature= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```

* **requestBody:** 

	symbol=BTCUSDT   
	&side=BUY   
	&type=LIMIT   
	&timeInForce=GTC   
	&quantity=1   
	&price=9000  
	&recvWindow=5000   
	&timestamp=1591702613943

### 示例 3: 混合使用 query string 与 request body

> **示例3:**

> **HMAC SHA256 签名:**

```shell
    $ echo -n "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943" | openssl dgst -sha256 -hmac "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
    (stdin)= 3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9
```


> **curl 调用:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: dbefbc809e3e83c283a984c3a1459732ea7db1360ca80c5c2c8867408d28cc83" -X POST 'https://fapi.asterdex.com/fapi/v1/order?symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC' -d 'quantity=1&price=9000&recvWindow=5000&timestamp=1591702613943&signature=3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9'
```

* **queryString:** symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC
* **requestBody:** quantity=1&price=9000&recvWindow=5000&timestamp= 1591702613943

请注意，示例3中的签名有些许不同，在"GTC"和"quantity=1"之间**没有**"&"字符。





## 公开API参数
### 术语解释
* `base asset` 指一个交易对的交易对象，即写在靠前部分的资产名
* `quote asset` 指一个交易对的定价资产，即写在靠后部分资产名


### 枚举定义

**交易对类型:**

* FUTURE 期货

**合约类型 (contractType):**

* PERPETUAL 永续合约


**合约状态 (contractStatus, status):**

* PENDING_TRADING   待上市
* TRADING          	交易中
* PRE_SETTLE			预结算
* SETTLING			结算中
* CLOSE				已下架


**订单状态 (status):**

* NEW 新建订单
* PARTIALLY_FILLED  部分成交
* FILLED  全部成交
* CANCELED  已撤销
* REJECTED 订单被拒绝
* EXPIRED 订单过期(根据timeInForce参数规则)

**订单种类 (orderTypes, type):**

* LIMIT 限价单
* MARKET 市价单
* STOP 止损限价单
* STOP_MARKET 止损市价单
* TAKE_PROFIT 止盈限价单
* TAKE_PROFIT_MARKET 止盈市价单
* TRAILING_STOP_MARKET 跟踪止损单

**订单方向 (side):**

* BUY 买入
* SELL 卖出

**持仓方向:**

* BOTH 单一持仓方向
* LONG 多头(双向持仓下)
* SHORT 空头(双向持仓下)

**有效方式 (timeInForce):**

* GTC - Good Till Cancel 成交为止
* IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
* FOK - Fill or Kill 无法全部立即成交就撤销
* GTX - Good Till Crossing 无法成为挂单方就撤销
* HIDDEN - HIDDEN 该类型订单在订单薄里不可见

**条件价格触发类型 (workingType)**

* MARK_PRICE
* CONTRACT_PRICE 

**响应类型 (newOrderRespType)**

* ACK
* RESULT

**K线间隔:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**限制种类 (rateLimitType)**

> REQUEST_WEIGHT

```javascript
  {
  	"rateLimitType": "REQUEST_WEIGHT",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 2400
  }
```

> ORDERS

```javascript
  {
  	"rateLimitType": "ORDERS",
  	"interval": "MINUTE",
  	"intervalNum": 1,
  	"limit": 1200
   }
```

* REQUESTS_WEIGHT  单位时间请求权重之和上限

* ORDERS    单位时间下单(撤单)次数上限


**限制间隔**

* MINUTE



## 过滤器
过滤器，即Filter，定义了一系列交易规则。
共有两类，分别是针对交易对的过滤器`symbol filters`，和针对整个交易所的过滤器`exchange filters`(暂不支持)

### 交易对过滤器
#### PRICE_FILTER 价格过滤器

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "PRICE_FILTER",
    "minPrice": "0.00000100",
    "maxPrice": "100000.00000000",
    "tickSize": "0.00000100"
  }
```

价格过滤器用于检测order订单中price参数的合法性

* `minPrice` 定义了 `price`/`stopPrice` 允许的最小值
* `maxPrice` 定义了 `price`/`stopPrice` 允许的最大值。
* `tickSize` 定义了 `price`/`stopPrice` 的步进间隔，即price必须等于minPrice+(tickSize的整数倍)
以上每一项均可为0，为0时代表这一项不再做限制。

逻辑伪代码如下：

* `price` >= `minPrice`
* `price` <= `maxPrice`
* (`price`-`minPrice`) % `tickSize` == 0



#### LOT_SIZE 订单尺寸

> */exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "LOT_SIZE",
    "minQty": "0.00100000",
    "maxQty": "100000.00000000",
    "stepSize": "0.00100000"
  }
```

lots是拍卖术语，这个过滤器对订单中的`quantity`也就是数量参数进行合法性检查。包含三个部分：

* `minQty` 表示 `quantity` 允许的最小值.
* `maxQty` 表示 `quantity` 允许的最大值
* `stepSize` 表示 `quantity`允许的步进值。

逻辑伪代码如下：

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0


#### MARKET_LOT_SIZE 市价订单尺寸
参考LOT_SIZE，区别仅在于对市价单还是限价单生效

#### MAX_NUM_ORDERS 最多订单数


> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "MAX_NUM_ORDERS",
    "limit": 200
  }
```

定义了某个交易对最多允许的挂单数量(不包括已关闭的订单)

普通订单与条件订单均计算在内


#### MAX_NUM_ALGO_ORDERS 最多条件订单数

> **/exchangeInfo format:**

```javascript
  {
    "filterType": "MAX_NUM_ALGO_ORDERS",
    "limit": 100
  }
```

定义了某个交易对最多允许的条件订单的挂单数量(不包括已关闭的订单)。   

条件订单目前包括`STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET`, 和 `TRAILING_STOP_MARKET`


#### PERCENT_PRICE 价格振幅过滤器

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "PERCENT_PRICE",
    "multiplierUp": "1.1500",
    "multiplierDown": "0.8500",
    "multiplierDecimal": 4
  }
```

`PERCENT_PRICE` 定义了基于标记价格计算的挂单价格的可接受区间.

挂单价格必须同时满足以下条件：

* 买单: `price` <= `markPrice` * `multiplierUp`
* 卖单: `price` >= `markPrice` * `multiplierDown`


#### MIN_NOTIONAL 最小名义价值

> **/exchangeInfo 响应中的格式:**

```javascript
  {
    "filterType": "MIN_NOTIONAL",
    "notioanl": "1"
  }
```

MIN_NOTIONAL过滤器定义了交易对订单所允许的最小名义价值(成交额)。
订单的名义价值是`价格`*`数量`。 
由于`MARKET`订单没有价格，因此会使用 mark price 计算。   






---


# 行情接口
## 测试服务器连通性 PING
``
GET /fapi/v1/ping
``

> **响应:**

```javascript
{}
```

测试能否联通

**权重:**
1

**参数:**
NONE



## 获取服务器时间

> **响应:**

```javascript
{
  "serverTime": 1499827319559 // 当前的系统时间
}
```

``
GET /fapi/v1/time
``

获取服务器时间

**权重:**
1

**参数:**
NONE


## 获取交易规则和交易对

> **响应:**

```javascript
{
	"exchangeFilters": [],
 	"rateLimits": [ // API访问的限制
 		{
 			"interval": "MINUTE", // 按照分钟计算
   			"intervalNum": 1, // 按照1分钟计算
   			"limit": 2400, // 上限次数
   			"rateLimitType": "REQUEST_WEIGHT" // 按照访问权重来计算
   		},
  		{
  			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 1200,
   			"rateLimitType": "ORDERS" // 按照订单数量来计算
   		}
   	],
 	"serverTime": 1565613908500, // 请忽略。如果需要获取当前系统时间，请查询接口 “GET /fapi/v1/time”
 	"assets": [ // 资产信息
 		{
 			"asset": "BUSD",
   			"marginAvailable": true, // 是否可用作保证金
   			"autoAssetExchange": 0 // 保证金资产自动兑换阈值
   		},
 		{
 			"asset": "USDT",
   			"marginAvailable": true, // 是否可用作保证金
   			"autoAssetExchange": 0 // 保证金资产自动兑换阈值
   		},
 		{
 			"asset": "BNB",
   			"marginAvailable": false, // 是否可用作保证金
   			"autoAssetExchange": null // 保证金资产自动兑换阈值
   		}
   	],
 	"symbols": [ // 交易对信息
 		{
 			"symbol": "BLZUSDT",  // 交易对
 			"pair": "BLZUSDT",  // 标的交易对
 			"contractType": "PERPETUAL",	// 合约类型
 			"deliveryDate": 4133404800000,  // 交割日期
 			"onboardDate": 1598252400000,	  // 上线日期
 			"status": "TRADING",  // 交易对状态
 			"maintMarginPercent": "2.5000",  // 请忽略
 			"requiredMarginPercent": "5.0000", // 请忽略
 			"baseAsset": "BLZ",  // 标的资产
 			"quoteAsset": "USDT", // 报价资产
 			"marginAsset": "USDT", // 保证金资产
 			"pricePrecision": 5,  // 价格小数点位数(仅作为系统精度使用，注意同tickSize 区分）
 			"quantityPrecision": 0,  // 数量小数点位数(仅作为系统精度使用，注意同stepSize 区分）
 			"baseAssetPrecision": 8,  // 标的资产精度
 			"quotePrecision": 8,  // 报价资产精度
 			"underlyingType": "COIN",
 			"underlyingSubType": ["STORAGE"],
 			"settlePlan": 0,
 			"triggerProtect": "0.15", // 开启"priceProtect"的条件订单的触发阈值
 			"filters": [
 				{
 					"filterType": "PRICE_FILTER", // 价格限制
     				"maxPrice": "300", // 价格上限, 最大价格
     				"minPrice": "0.0001", // 价格下限, 最小价格
     				"tickSize": "0.0001" // 订单最小价格间隔
     			},
    			{
    				"filterType": "LOT_SIZE", // 数量限制
     				"maxQty": "10000000", // 数量上限, 最大数量
     				"minQty": "1", // 数量下限, 最小数量
     				"stepSize": "1" // 订单最小数量间隔
     			},
    			{
    				"filterType": "MARKET_LOT_SIZE", // 市价订单数量限制
     				"maxQty": "590119", // 数量上限, 最大数量
     				"minQty": "1", // 数量下限, 最小数量
     				"stepSize": "1" // 允许的步进值
     			},
     			{
    				"filterType": "MAX_NUM_ORDERS", // 最多订单数限制
    				"limit": 200
  				},
  				{
    				"filterType": "MAX_NUM_ALGO_ORDERS", // 最多条件订单数限制
    				"limit": 100
  				},
  				{
  					"filterType": "MIN_NOTIONAL",  // 最小名义价值
  					"notional": "1", 
  				},
  				{
    				"filterType": "PERCENT_PRICE", // 价格比限制
    				"multiplierUp": "1.1500", // 价格上限百分比
    				"multiplierDown": "0.8500", // 价格下限百分比
    				"multiplierDecimal": 4
    			}
   			],
 			"OrderType": [ // 订单类型
   				"LIMIT",  // 限价单
   				"MARKET",  // 市价单
   				"STOP", // 止损单
   				"STOP_MARKET", // 止损市价单
   				"TAKE_PROFIT", // 止盈单
   				"TAKE_PROFIT_MARKET", // 止盈暑市价单
   				"TRAILING_STOP_MARKET" // 跟踪止损市价单
   			],
   			"timeInForce": [ // 有效方式
   				"GTC", // 成交为止, 一直有效
   				"IOC", // 无法立即成交(吃单)的部分就撤销
   				"FOK", // 无法全部立即成交就撤销
   				"GTX" // 无法成为挂单方就撤销
				"HIDDEN" //  该类型订单在订单薄里不可见
 			],
 			"liquidationFee": "0.010000",	// 强平费率
   			"marketTakeBound": "0.30",	// 市价吃单(相对于标记价格)允许可造成的最大价格偏离比例
 		}
   	],
	"timezone": "UTC" // 服务器所用的时间区域
}

```

``
GET /fapi/v1/exchangeInfo
``

获取交易规则和交易对

**权重:**
1

**参数:**
NONE



## 深度信息

> **响应:**

```javascript
{
  "lastUpdateId": 1027024,
  "E": 1589436922972,   // 消息时间
  "T": 1589436922959,   // 撮合引擎时间
  "bids": [				// 买单
    [
      "4.00000000",     // 价格
      "431.00000000"    // 数量
    ]
  ],
  "asks": [				// 卖单
    [
      "4.00000200",		// 价格
      "12.00000000"		// 数量
    ]
  ]
}
```

``
GET /fapi/v1/depth
``

**权重:**

limit         | 权重
------------  | ------------
5, 10, 20, 50 | 2
100           | 5
500           | 10
1000          | 20

**参数:**

 名称  |  类型  | 是否必需 |                            描述
------ | ------ | -------- | -----------------------------------------------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认 500; 可选值:[5, 10, 20, 50, 100, 500, 1000]



## 近期成交

> **响应:**

```javascript
[
  {
    "id": 28457,				// 成交ID
    "price": "4.00000100",		// 成交价格
    "qty": "12.00000000",		// 成交量
    "quoteQty": "48.00",		// 成交额
    "time": 1499865549590,		// 时间
    "isBuyerMaker": true		// 买方是否为挂单方
  }
]
```

``
GET /fapi/v1/trades
``

获取近期订单簿成交

**权重:**
1

**参数:**

 名称  |  类型  | 是否必需 |          描述
------ | ------ | -------- | ----------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认:500，最大1000 

* 仅返回订单簿成交，即不会返回保险基金和自动减仓(ADL)成交

## 查询历史成交(MARKET_DATA)

> **响应:**

```javascript
[
  {
    "id": 28457,				// 成交ID
    "price": "4.00000100",		// 成交价格
    "qty": "12.00000000",		// 成交量
    "quoteQty": "48.00",		// 成交额
    "time": 1499865549590,		// 时间
    "isBuyerMaker": true		// 买方是否为挂单方
  }
]
```

``
GET /fapi/v1/historicalTrades
``

查询订单簿历史成交

**权重:**
20

**参数:**

 名称  |  类型  | 是否必需 |                      描述
------ | ------ | -------- | ----------------------------------------------
symbol | STRING | YES      | 交易对
limit  | INT    | NO       | 默认值:500 最大值:1000.
fromId | LONG   | NO       | 从哪一条成交id开始返回. 缺省返回最近的成交记录

* 仅返回订单簿成交，即不会返回保险基金和自动减仓(ADL)成交

## 近期成交(归集)

> **响应:**

```javascript
[
  {
    "a": 26129,         // 归集成交ID
    "p": "0.01633102",  // 成交价
    "q": "4.70443515",  // 成交量
    "f": 27781,         // 被归集的首个成交ID
    "l": 27781,         // 被归集的末个成交ID
    "T": 1498793709153, // 成交时间
    "m": true,          // 是否为主动卖出单
  }
]
```

``
GET /fapi/v1/aggTrades
``

归集交易与逐笔交易的区别在于，同一价格、同一方向、同一时间(按秒计算)的订单簿trade会被聚合为一条

**权重:**
20

**参数:**

  名称    |  类型  | 是否必需 |                描述
--------- | ------ | -------- | ----------------------------------
symbol    | STRING | YES      | 交易对
fromId    | LONG   | NO       | 从包含fromID的成交开始返回结果
startTime | LONG   | NO       | 从该时刻之后的成交记录开始返回结果
endTime   | LONG   | NO       | 返回该时刻为止的成交记录
limit     | INT    | NO       | 默认 500; 最大 1000.

* 如果同时发送`startTime`和`endTime`，间隔必须小于一小时
* 如果没有发送任何筛选参数(`fromId`, `startTime`, `endTime`)，默认返回最近的成交记录
* 保险基金和自动减仓(ADL)成交不属于订单簿成交，故不会被归并聚合


## K线数据

> **响应:**

```javascript
[
  [
    1499040000000,      // 开盘时间
    "0.01634790",       // 开盘价
    "0.80000000",       // 最高价
    "0.01575800",       // 最低价
    "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
    "148976.11427815",  // 成交量
    1499644799999,      // 收盘时间
    "2434.19055334",    // 成交额
    308,                // 成交笔数
    "1756.87402397",    // 主动买入成交量
    "28.46694368",      // 主动买入成交额
    "17928899.62484339" // 请忽略该参数
  ]
]
```

``
GET /fapi/v1/klines
``

每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
symbol    | STRING | YES      | 交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500.

* 缺省返回最近的数据



## 价格指数K线数据

> **响应:**

```javascript
[
  [
    1591256400000,      	// 开盘时间
    "9653.69440000",    	// 开盘价
    "9653.69640000",     	// 最高价
    "9651.38600000",     	// 最低价
    "9651.55200000",     	// 收盘价(当前K线未结束的即为最新价)
    "0	", 					// 请忽略
    1591256459999,      	// 收盘时间
    "0",    				// 请忽略
    60,                		// 构成记录数
    "0",    				// 请忽略
    "0",      				// 请忽略
    "0" 					// 请忽略
  ]
]
```

``
GET /fapi/v1/indexPriceKlines
``

每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
pair    	| STRING | YES      | 标的交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500

* 缺省返回最近的数据


## 标记价格K线数据

> **响应:**

```javascript
[
  [
    1591256400000,      	// 开盘时间
    "9653.69440000",    	// 开盘价
    "9653.69640000",     	// 最高价
    "9651.38600000",     	// 最低价
    "9651.55200000",     	// 收盘价(当前K线未结束的即为最新价)
    "0	", 					// 请忽略
    1591256459999,      	// 收盘时间
    "0",    				// 请忽略
    60,                		// 构成记录数
    "0",    				// 请忽略
    "0",      				// 请忽略
    "0" 					// 请忽略
  ]
]
```

``
GET /fapi/v1/markPriceKlines
``
每根K线的开盘时间可视为唯一ID

**权重:** 取决于请求中的LIMIT参数

LIMIT参数 | 权重
---|---
[1,100) | 1
[100, 500) | 2
[500, 1000] | 5
> 1000 | 10

**参数:**

  名称    |  类型  | 是否必需 |          描述
--------- | ------ | -------- | ----------------------
symbol   	| STRING | YES      | 交易对
interval  | ENUM   | YES      | 时间间隔
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:500 最大值:1500

* 缺省返回最近的数据


## 最新标记价格和资金费率 

> **响应:**

```javascript
{
    "symbol": "BTCUSDT",				// 交易对
    "markPrice": "11793.63104562",		// 标记价格
    "indexPrice": "11781.80495970",		// 指数价格
    "estimatedSettlePrice": "11781.16138815",  // 预估结算价,仅在交割开始前最后一小时有意义
    "lastFundingRate": "0.00038246",	// 最近更新的资金费率
    "nextFundingTime": 1597392000000,	// 下次资金费时间
    "interestRate": "0.00010000",		// 标的资产基础利率
    "time": 1597370495002				// 更新时间
}
```

> **当不指定symbol时相应**

```javascript
[
	{
    	"symbol": "BTCUSDT",			// 交易对
    	"markPrice": "11793.63104562",	// 标记价格
    	"indexPrice": "11781.80495970",	// 指数价格
    	"estimatedSettlePrice": "11781.16138815",  // 预估结算价,仅在交割开始前最后一小时有意义
    	"lastFundingRate": "0.00038246",	// 最近更新的资金费率
    	"nextFundingTime": 1597392000000,	// 下次资金费时间
    	"interestRate": "0.00010000",		// 标的资产基础利率
    	"time": 1597370495002				// 更新时间
	}
]
```


``
GET /fapi/v1/premiumIndex
``

采集各大交易所数据加权平均

**权重:**
1

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对


## 查询资金费率历史

> **响应:**

```javascript
[
	{
    	"symbol": "BTCUSDT",			// 交易对
    	"fundingRate": "-0.03750000",	// 资金费率
    	"fundingTime": 1570608000000,	// 资金费时间
	},
	{
   		"symbol": "BTCUSDT",
    	"fundingRate": "0.00010000",
    	"fundingTime": 1570636800000,
	}
]
```

``
GET /fapi/v1/fundingRate
``

**权重:**
1

**参数:**

  名称    |  类型  | 是否必需 |                         描述
--------- | ------ | -------- | -----------------------------------------------------
symbol    | STRING | NO      | 交易对
startTime | LONG   | NO       | 起始时间
endTime   | LONG   | NO       | 结束时间
limit     | INT    | NO       | 默认值:100 最大值:1000

* 如果 `startTime` 和 `endTime` 都未发送, 返回最近 `limit` 条数据.
* 如果 `startTime` 和 `endTime` 之间的数据量大于 `limit`, 返回 `startTime` + `limit`情况下的数据。

## 查询资金费率配置

> **响应:**

```javascript
[
	{
		"symbol": "INJUSDT",            // 交易对
		"interestRate": "0.00010000",   // 利率
		"time": 1756197479000,          // 更新时间
		"fundingIntervalHours": 8,      // 资金费间隔小时数
		"fundingFeeCap": 0.03,          // 资金费上限
		"fundingFeeFloor": -0.03        // 资金费下限
	},
	{
		"symbol": "ZORAUSDT",
		"interestRate": "0.00005000",
		"time": 1756197479000,
		"fundingIntervalHours": 4,
		"fundingFeeCap": 0.02,
		"fundingFeeFloor": -0.02
	}
]
```

``
GET /fapi/v1/fundingInfo
``

**权重:**
1

**参数:**

  名称    |  类型  | 是否必需 |                         描述
--------- | ------ | -------- | -----------------------------------------------------
symbol    | STRING | NO      | 交易对


## 24hr价格变动情况

> **响应:**

```javascript
{
  "symbol": "BTCUSDT",
  "priceChange": "-94.99999800",    //24小时价格变动
  "priceChangePercent": "-95.960",  //24小时价格变动百分比
  "weightedAvgPrice": "0.29628482", //加权平均价
  "lastPrice": "4.00000200",        //最近一次成交价
  "lastQty": "200.00000000",        //最近一次成交额
  "openPrice": "99.00000000",       //24小时内第一次成交的价格
  "highPrice": "100.00000000",      //24小时最高价
  "lowPrice": "0.10000000",         //24小时最低价
  "volume": "8913.30000000",        //24小时成交量
  "quoteVolume": "15.30000000",     //24小时成交额
  "openTime": 1499783499040,        //24小时内，第一笔交易的发生时间
  "closeTime": 1499869899040,       //24小时内，最后一笔交易的发生时间
  "firstId": 28385,   // 首笔成交id
  "lastId": 28460,    // 末笔成交id
  "count": 76         // 成交笔数
}
```

> 或(当不发送交易对信息)

```javascript
[
	{
  		"symbol": "BTCUSDT",
  		"priceChange": "-94.99999800",    //24小时价格变动
  		"priceChangePercent": "-95.960",  //24小时价格变动百分比
  		"weightedAvgPrice": "0.29628482", //加权平均价
  		"lastPrice": "4.00000200",        //最近一次成交价
  		"lastQty": "200.00000000",        //最近一次成交额
  		"openPrice": "99.00000000",       //24小时内第一次成交的价格
  		"highPrice": "100.00000000",      //24小时最高价
  		"lowPrice": "0.10000000",         //24小时最低价
  		"volume": "8913.30000000",        //24小时成交量
  		"quoteVolume": "15.30000000",     //24小时成交额
  		"openTime": 1499783499040,        //24小时内，第一笔交易的发生时间
  		"closeTime": 1499869899040,       //24小时内，最后一笔交易的发生时间
  		"firstId": 28385,   // 首笔成交id
  		"lastId": 28460,    // 末笔成交id
  		"count": 76         // 成交笔数
    }
]
```

``
GET /fapi/v1/ticker/24hr
``

请注意，不携带symbol参数会返回全部交易对数据，不仅数据庞大，而且权重极高

**权重:**
* 带symbol为`1`
* 不带为`40`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息


## 最新价格

> **响应:**

```javascript
{
  "symbol": "LTCBTC",		// 交易对
  "price": "4.00000200",		// 价格
  "time": 1589437530011   // 撮合引擎时间
}
```

> 或(当不发送symbol)

```javascript
[
	{
  		"symbol": "BTCUSDT",	// 交易对
  		"price": "6000.01",		// 价格
  		"time": 1589437530011   // 撮合引擎时间
	}
]
```

``
GET /fapi/v1/ticker/price
``

返回最近价格

**权重:**
* 单交易对`1`
* 无交易对`2`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息


## 当前最优挂单

> **响应:**

```javascript
{
  "symbol": "BTCUSDT", // 交易对
  "bidPrice": "4.00000000", //最优买单价
  "bidQty": "431.00000000", //挂单量
  "askPrice": "4.00000200", //最优卖单价
  "askQty": "9.00000000", //挂单量
  "time": 1589437530011   // 撮合引擎时间
}
```
> 或(当不发送symbol)

```javascript
[
	{
  		"symbol": "BTCUSDT", // 交易对
  		"bidPrice": "4.00000000", //最优买单价
  		"bidQty": "431.00000000", //挂单量
  		"askPrice": "4.00000200", //最优卖单价
  		"askQty": "9.00000000", //挂单量
  		"time": 1589437530011   // 撮合引擎时间
	}
]
```

``
GET /fapi/v1/ticker/bookTicker
``

返回当前最优的挂单(最高买单，最低卖单)

**权重:**
* 单交易对`1`   
* 无交易对`2`

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol | STRING | NO       | 交易对

* 不发送交易对参数，则会返回所有交易对信息




# Websocket 行情推送

* 本篇所列出的所有wss接口baseurl: **wss://fstream.asterdex.com**
* 订阅单一stream格式为 **/ws/\<streamName\>**
* 组合streams的URL格式为 **/stream?streams=\<streamName1\>/\<streamName2\>/\<streamName3\>**
* 订阅组合streams时，事件payload会以这样的格式封装 **{"stream":"\<streamName\>","data":\<rawPayload\>}**
* stream名称中所有交易对均为**小写**
* 每个链接有效期不超过24小时，请妥善处理断线重连。
* 服务端每5分钟会发送ping帧，客户端应当在15分钟内回复pong帧，否则服务端会主动断开链接。允许客户端发送不成对的pong帧(即客户端可以以高于15分钟每次的频率发送pong帧保持链接)。
* Websocket服务器每秒最多接受10个订阅消息。
* 如果用户发送的消息超过限制，连接会被断开连接。反复被断开连接的IP有可能被服务器屏蔽。
* 单个连接最多可以订阅 **200** 个Streams。




## 实时订阅/取消数据流

* 以下数据可以通过websocket发送以实现订阅或取消订阅数据流。示例如下。
* 响应内容中的`id`是无符号整数，作为往来信息的唯一标识。

### 订阅一个信息流

> **响应**

  ```javascript
  {
    "result": null,
    "id": 1
  }
  ```

* **请求**

  	{    
    	"method": "SUBSCRIBE",    
    	"params":     
    	[   
      	"btcusdt@aggTrade",    
      	"btcusdt@depth"     
    	],    
    	"id": 1   
  	}



### 取消订阅一个信息流

> **响应**
  
  ```javascript
  {
    "result": null,
    "id": 312
  }
  ```

* **请求**

  {   
    "method": "UNSUBSCRIBE",    
    "params":     
    [    
      "btcusdt@depth"   
    ],    
    "id": 312   
  }



### 已订阅信息流

> **响应**
  
  ```javascript
  {
    "result": [
      "btcusdt@aggTrade"
    ],
    "id": 3
  }
  ```


* **请求**

  {   
    "method": "LIST_SUBSCRIPTIONS",    
    "id": 3   
  }     
 


### 设定属性
当前，唯一可以设置的属性是设置是否启用`combined`("组合")信息流。   
当使用`/ws/`("原始信息流")进行连接时，combined属性设置为`false`，而使用 `/stream/`进行连接时则将属性设置为`true`。


> **响应**
  
  ```javascript
  {
    "result": null
    "id": 5
  }
  ```

* **请求**

  {    
    "method": "SET_PROPERTY",    
    "params":     
    [   
      "combined",    
      true   
    ],    
    "id": 5   
  }




### 检索属性

> **响应**

  ```javascript
  {
    "result": true, // Indicates that combined is set to true.
    "id": 2
  }
  ```
  
* **请求**
  
  {   
    "method": "GET_PROPERTY",    
    "params":     
    [   
      "combined"   
    ],    
    "id": 2   
  }   
 



### 错误信息

错误信息 | 描述
---|---
{"code": 0, "msg": "Unknown property"} |  `SET_PROPERTY` 或 `GET_PROPERTY`中应用的参数无效
{"code": 1, "msg": "Invalid value type: expected Boolean"} | 仅接受`true`或`false`
{"code": 2, "msg": "Invalid request: property name must be a string"}| 提供的属性名无效
{"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"}| 参数`id`未提供或`id`值是无效类型
{"code": 2, "msg": "Invalid request: unknown variant %s, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28"} | 错字提醒，或提供的值不是预期类型
{"code": 2, "msg": "Invalid request: too many parameters"}| 数据中提供了不必要参数
{"code": 2, "msg": "Invalid request: property name must be a string"} | 未提供属性名
{"code": 2, "msg": "Invalid request: missing field `method` at line 1 column 73"} | 数据未提供`method`
{"code":3,"msg":"Invalid JSON: expected value at line %s column %s"} | JSON 语法有误.




## 最新合约价格
aggTrade中的价格'p'或ticker/miniTicker中的价格'c'均可以作为最新成交价。

## 归集交易

> **Payload:**

```javascript
{
  "e": "aggTrade",  // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBUSDT",    // 交易对
  "a": 5933014,		// 归集成交 ID
  "p": "0.001",     // 成交价格
  "q": "100",       // 成交量
  "f": 100,         // 被归集的首个交易ID
  "l": 105,         // 被归集的末次交易ID
  "T": 123456785,   // 成交时间
  "m": true         // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
}
```

同一价格、同一方向、同一时间(100ms计算)的trade会被聚合为一条.

**Stream Name:**       
``<symbol>@aggTrade``

**Update Speed:** 100ms





## 最新标记价格

> **Payload:**

```javascript
  {
    "e": "markPriceUpdate",  	// 事件类型
    "E": 1562305380000,      	// 事件时间
    "s": "BTCUSDT",          	// 交易对
    "p": "11794.15000000",   	// 标记价格
    "i": "11784.62659091",		// 现货指数价格
    "P": "11784.25641265",		// 预估结算价,仅在结算前最后一小时有参考价值
    "r": "0.00038167",       	// 资金费率
    "T": 1562306400000       	// 下次资金时间
  }
```


**Stream Name:**    
``<symbol>@markPrice`` 或 ``<symbol>@markPrice@1s``

**Update Speed:** 3000ms 或 1000ms






## 全市场最新标记价格

> **Payload:**

```javascript
[
  {
    "e": "markPriceUpdate",  	// 事件类型
    "E": 1562305380000,      	// 事件时间
    "s": "BTCUSDT",          	// 交易对
    "p": "11185.87786614",   	// 标记价格
    "i": "11784.62659091"		// 现货指数价格
    "P": "11784.25641265",		// 预估结算价,仅在结算前最后一小时有参考价值
    "r": "0.00030000",       	// 资金费率
    "T": 1562306400000       	// 下个资金时间
  }
]
```


**Stream Name:**    
``!markPrice@arr`` 或 ``!markPrice@arr@1s``

**Update Speed:** 3000ms 或 1000ms





## K线

> **Payload:**

```javascript
{
  "e": "kline",     // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBUSDT",    // 交易对
  "k": {
    "t": 123400000, // 这根K线的起始时间
    "T": 123460000, // 这根K线的结束时间
    "s": "BNBUSDT",  // 交易对
    "i": "1m",      // K线间隔
    "f": 100,       // 这根K线期间第一笔成交ID
    "L": 200,       // 这根K线期间末一笔成交ID
    "o": "0.0010",  // 这根K线期间第一笔成交价
    "c": "0.0020",  // 这根K线期间末一笔成交价
    "h": "0.0025",  // 这根K线期间最高成交价
    "l": "0.0015",  // 这根K线期间最低成交价
    "v": "1000",    // 这根K线期间成交量
    "n": 100,       // 这根K线期间成交笔数
    "x": false,     // 这根K线是否完结(是否已经开始下一根K线)
    "q": "1.0000",  // 这根K线期间成交额
    "V": "500",     // 主动买入的成交量
    "Q": "0.500",   // 主动买入的成交额
    "B": "123456"   // 忽略此参数
  }
}
```

K线stream逐秒推送所请求的K线种类(最新一根K线)的更新。推送间隔250毫秒(如有刷新)

**订阅Kline需要提供间隔参数，最短为分钟线，最长为月线。支持以下间隔:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**Stream Name:**    
``<symbol>@kline_<interval>``

**Update Speed:** 250ms




## 按Symbol的精简Ticker

> **Payload:**

```javascript
  {
    "e": "24hrMiniTicker",  // 事件类型
    "E": 123456789,         // 事件时间(毫秒)
    "s": "BNBUSDT",          // 交易对
    "c": "0.0025",          // 最新成交价格
    "o": "0.0010",          // 24小时前开始第一笔成交价格
    "h": "0.0025",          // 24小时内最高成交价
    "l": "0.0010",          // 24小时内最低成交价
    "v": "10000",           // 成交量
    "q": "18"               // 成交额
  }
```

按Symbol刷新的24小时精简ticker信息.

**Stream Name:**     
``<symbol>@miniTicker`

**Update Speed:** 500ms



## 全市场的精简Ticker

> **Payload:**

```javascript
[  
  {
    "e": "24hrMiniTicker",  // 事件类型
    "E": 123456789,         // 事件时间(毫秒)
    "s": "BNBUSDT",          // 交易对
    "c": "0.0025",          // 最新成交价格
    "o": "0.0010",          // 24小时前开始第一笔成交价格
    "h": "0.0025",          // 24小时内最高成交价
    "l": "0.0010",          // 24小时内最低成交价
    "v": "10000",           // 成交量
    "q": "18"               // 成交额
  }
]
```

所有symbol24小时精简ticker信息.需要注意的是，只有发生变化的ticker更新才会被推送。

**Stream Name:**     
`!miniTicker@arr`

**Update Speed:** 1000ms




## 按Symbol的完整Ticker


> **Payload:**

```javascript
{
  "e": "24hrTicker",  // 事件类型
  "E": 123456789,     // 事件时间
  "s": "BNBUSDT",      // 交易对
  "p": "0.0015",      // 24小时价格变化
  "P": "250.00",      // 24小时价格变化(百分比)
  "w": "0.0018",      // 平均价格
  "c": "0.0025",      // 最新成交价格
  "Q": "10",          // 最新成交价格上的成交量
  "o": "0.0010",      // 24小时内第一比成交的价格
  "h": "0.0025",      // 24小时内最高成交价
  "l": "0.0010",      // 24小时内最低成交价
  "v": "10000",       // 24小时内成交量
  "q": "18",          // 24小时内成交额
  "O": 0,             // 统计开始时间
  "C": 86400000,      // 统计关闭时间
  "F": 0,             // 24小时内第一笔成交交易ID
  "L": 18150,         // 24小时内最后一笔成交交易ID
  "n": 18151          // 24小时内成交数
}
```

按Symbol刷新的24小时完整ticker信息

**Stream Name:**     
``<symbol>@ticker``

**Update Speed:** 500ms



## 全市场的完整Ticker


> **Payload:**

```javascript
[
	{
	  "e": "24hrTicker",  // 事件类型
	  "E": 123456789,     // 事件时间
	  "s": "BNBUSDT",      // 交易对
	  "p": "0.0015",      // 24小时价格变化
	  "P": "250.00",      // 24小时价格变化(百分比)
	  "w": "0.0018",      // 平均价格
	  "c": "0.0025",      // 最新成交价格
	  "Q": "10",          // 最新成交价格上的成交量
	  "o": "0.0010",      // 24小时内第一比成交的价格
	  "h": "0.0025",      // 24小时内最高成交价
	  "l": "0.0010",      // 24小时内最低成交价
	  "v": "10000",       // 24小时内成交量
	  "q": "18",          // 24小时内成交额
	  "O": 0,             // 统计开始时间
	  "C": 86400000,      // 统计结束时间
	  "F": 0,             // 24小时内第一笔成交交易ID
	  "L": 18150,         // 24小时内最后一笔成交交易ID
	  "n": 18151          // 24小时内成交数
	}
]	
```

所有symbol 24小时完整ticker信息.需要注意的是，只有发生变化的ticker更新才会被推送。

**Stream Name:**     
``!ticker@arr``

**Update Speed:** 1000ms


## 按Symbol的最优挂单信息

> **Payload:**

```javascript
{
  "e":"bookTicker",		// 事件类型
  "u":400900217,     	// 更新ID
  "E": 1568014460893,	// 事件推送时间
  "T": 1568014460891,	// 撮合时间
  "s":"BNBUSDT",     	// 交易对
  "b":"25.35190000", 	// 买单最优挂单价格
  "B":"31.21000000", 	// 买单最优挂单数量
  "a":"25.36520000", 	// 卖单最优挂单价格
  "A":"40.66000000"  	// 卖单最优挂单数量
}
```


实时推送指定交易对最优挂单信息

**Stream Name:** `<symbol>@bookTicker`

**Update Speed:** 实时





## 全市场最优挂单信息

> **Payload:**

```javascript
{
  // Same as <symbol>@bookTicker payload
}
```

所有交易对交易对最优挂单信息

**Stream Name:** `!bookTicker`

**Update Speed:** 实时



##强平订单

> **Payload:**

```javascript
{

	"e":"forceOrder",                   // 事件类型
	"E":1568014460893,                  // 事件时间
	"o":{
	
		"s":"BTCUSDT",                   // 交易对
		"S":"SELL",                      // 订单方向
		"o":"LIMIT",                     // 订单类型
		"f":"IOC",                       // 有效方式
		"q":"0.014",                     // 订单数量
		"p":"9910",                      // 订单价格
		"ap":"9910",                     // 平均价格
		"X":"FILLED",                    // 订单状态
		"l":"0.014",                     // 订单最近成交量
		"z":"0.014",                     // 订单累计成交量
		"T":1568014460893,          	 // 交易时间
	
	}

}
```

推送特定`symbol`的强平订单快照信息。

1000ms内至多仅推送一条最近的强平订单作为快照

**Stream Name:**  ``<symbol>@forceOrder``

**Update Speed:** 1000ms





## 有限档深度信息

> **Payload:**

```javascript
{
  "e": "depthUpdate", 			// 事件类型
  "E": 1571889248277, 			// 事件时间
  "T": 1571889248276, 			// 交易时间
  "s": "BTCUSDT",
  "U": 390497796,
  "u": 390497878,
  "pu": 390497794,
  "b": [          				// 买方
    [
      "7403.89",  				// 价格
      "0.002"     				// 数量
    ],
    [
      "7403.90",
      "3.906"
    ],
    [
      "7404.00",
      "1.428"
    ],
    [
      "7404.85",
      "5.239"
    ],
    [
      "7405.43",
      "2.562"
    ]
  ],
  "a": [          				// 卖方
    [
      "7405.96",  				// 价格
      "3.340"     				// 数量
    ],
    [
      "7406.63",
      "4.525"
    ],
    [
      "7407.08",
      "2.475"
    ],
    [
      "7407.15",
      "4.800"
    ],
    [
      "7407.20",
      "0.175"
    ]
  ]
}
```

推送有限档深度信息。levels表示几档买卖单信息, 可选 5/10/20档

**Stream Names:** `<symbol>@depth<levels>` 或 `<symbol>@depth<levels>@500ms` 或 `<symbol>@depth<levels>@100ms`.  

**Update Speed:** 250ms 或 500ms 或 100ms




## 增量深度信息

> **Payload:**

```javascript
{
  "e": "depthUpdate", 	// 事件类型
  "E": 123456789,     	// 事件时间
  "T": 123456788,     	// 撮合时间
  "s": "BNBUSDT",      	// 交易对
  "U": 157,           	// 从上次推送至今新增的第一个 update Id
  "u": 160,           	// 从上次推送至今新增的最后一个 update Id
  "pu": 149,          	// 上次推送的最后一个update Id(即上条消息的‘u’)
  "b": [              	// 变动的买单深度
    [
      "0.0024",       	// 价格
      "10"           	// 数量
    ]
  ],
  "a": [              	// 变动的卖单深度
    [
      "0.0026",       	// 价格
      "100"          	// 数量
    ]
  ]
}
```

orderbook的变化部分，推送间隔250毫秒,500毫秒，100毫秒(如有刷新)

**Stream 名称:**     
``<symbol>@depth`` OR ``<symbol>@depth@500ms`` OR ``<symbol>@depth@100ms``

**Update Speed:** 250ms 或 500ms 或 100ms


## 如何正确在本地维护一个orderbook副本
1. 订阅 **wss://fstream.asterdex.com/stream?streams=btcusdt@depth**
2. 开始缓存收到的更新。同一个价位，后收到的更新覆盖前面的。
3. 访问Rest接口 **https://fapi.asterdex.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000**获得一个1000档的深度快照
4. 将目前缓存到的信息中`u`< 步骤3中获取到的快照中的`lastUpdateId`的部分丢弃(丢弃更早的信息，已经过期)。
5. 将深度快照中的内容更新到本地orderbook副本中，并从websocket接收到的第一个`U` <= `lastUpdateId` **且** `u` >= `lastUpdateId` 的event开始继续更新本地副本。
6. 每一个新event的`pu`应该等于上一个event的`u`，否则可能出现了丢包，请从step3重新进行初始化。
7. 每一个event中的挂单量代表这个价格目前的挂单量**绝对值**，而不是相对变化。
8. 如果某个价格对应的挂单量为0，表示该价位的挂单已经撤单或者被吃，应该移除这个价位。




# 账户和交易接口

<aside class="warning">
考虑到剧烈行情下, RESTful接口可能存在查询延迟，我们强烈建议您优先从Websocket user data stream推送的消息来获取订单，成交，仓位等信息。
</aside>


## 更改持仓模式(TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/positionSide/dual (HMAC SHA256)
``

变换用户在 ***所有symbol*** 合约上的持仓模式：双向持仓或单向持仓。   

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
dualSidePosition | STRING   | YES      | "true": 双向持仓模式；"false": 单向持仓模式
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 查询持仓模式(USER_DATA)

> **响应:**

```javascript
{
	"dualSidePosition": true // "true": 双向持仓模式；"false": 单向持仓模式
}
```

``
GET /fapi/v1/positionSide/dual (HMAC SHA256)
``

查询用户目前在 ***所有symbol*** 合约上的持仓模式：双向持仓或单向持仓。     

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 更改联合保证金模式(TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/multiAssetsMargin (HMAC SHA256)
``

变换用户在 ***所有symbol*** 合约上的联合保证金模式：开启或关闭联合保证金模式。   

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
multiAssetsMargin | STRING   | YES      | "true": 联合保证金模式开启；"false": 联合保证金模式关闭
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 查询联合保证金模式(USER_DATA)

> **响应:**

```javascript
{
	"multiAssetsMargin": true // "true": 联合保证金模式开启；"false": 联合保证金模式关闭
}
```

``
GET /fapi/v1/multiAssetsMargin (HMAC SHA256)
``

查询用户目前在 ***所有symbol*** 合约上的联合保证金模式。      

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 下单 (TRADE)


> **响应:**

```javascript
{
 	"clientOrderId": "testOrder", // 用户自定义的订单号
 	"cumQty": "0",
 	"cumQuote": "0", // 成交金额
 	"executedQty": "0", // 成交量
 	"orderId": 22542179, // 系统订单号
 	"avgPrice": "0.00000",	// 平均成交价
 	"origQty": "10", // 原始委托数量
 	"price": "0", // 委托价格
 	"reduceOnly": false, // 仅减仓
 	"side": "SELL", // 买卖方向
 	"positionSide": "SHORT", // 持仓方向
 	"status": "NEW", // 订单状态
 	"stopPrice": "0", // 触发价，对`TRAILING_STOP_MARKET`无效
 	"closePosition": false,   // 是否条件全平仓
 	"symbol": "BTCUSDT", // 交易对
 	"timeInForce": "GTC", // 有效方法
 	"type": "TRAILING_STOP_MARKET", // 订单类型
 	"origType": "TRAILING_STOP_MARKET",  // 触发前订单类型
 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
 	"updateTime": 1566818724722, // 更新时间
 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
POST /fapi/v1/order  (HMAC SHA256)
``

**权重:**
1

**参数:**

名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ---
symbol           | STRING  | YES      | 交易对
side             | ENUM    | YES      | 买卖方向 `SELL`, `BUY`
positionSide     | ENUM	    | NO       | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT`  
type             | ENUM    | YES      | 订单类型 `LIMIT`, `MARKET`, `STOP`, `TAKE_PROFIT`, `STOP_MARKET`, `TAKE_PROFIT_MARKET`, `TRAILING_STOP_MARKET`
reduceOnly       | STRING  | NO       | `true`, `false`; 非双开模式下默认`false`；双开模式下不接受此参数； 使用`closePosition`不支持此参数。
quantity         | DECIMAL | NO     	 | 下单数量,使用`closePosition`不支持此参数。
price            | DECIMAL | NO       | 委托价格
newClientOrderId | STRING  | NO       | 用户自定义的订单号，不可以重复出现在挂单中。如空缺系统会自动赋值。必须满足正则规则 `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice        | DECIMAL | NO       | 触发价, 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
closePosition    | STRING  | NO       | `true`, `false`；触发后全部平仓，仅支持`STOP_MARKET`和`TAKE_PROFIT_MARKET`；不与`quantity`合用；自带只平仓效果，不与`reduceOnly` 合用
activationPrice  | DECIMAL | NO       | 追踪止损激活价格，仅`TRAILING_STOP_MARKET` 需要此参数, 默认为下单当前市场价格(支持不同`workingType`)
callbackRate     | DECIMAL | NO       | 追踪止损回调比例，可取值范围[0.1, 5],其中 1代表1% ,仅`TRAILING_STOP_MARKET` 需要此参数
timeInForce      | ENUM    | NO       | 有效方法
workingType      | ENUM    | NO       | stopPrice 触发类型: `MARK_PRICE`(标记价格), `CONTRACT_PRICE`(合约最新价). 默认 `CONTRACT_PRICE`
priceProtect | STRING | NO | 条件单触发保护："TRUE","FALSE", 默认"FALSE". 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", 默认 "ACK"
recvWindow       | LONG    | NO       |
timestamp        | LONG    | YES      |

根据 order `type`的不同，某些参数强制要求，具体如下:

Type                 |           强制要求的参数
----------------------------------- | ----------------------------------
`LIMIT`                             | `timeInForce`, `quantity`, `price`
`MARKET`                            | `quantity`
`STOP`, `TAKE_PROFIT`               | `quantity`,  `price`, `stopPrice`
`STOP_MARKET`, `TAKE_PROFIT_MARKET` | `stopPrice`
`TRAILING_STOP_MARKET`              | `callbackRate`



* 条件单的触发必须:
	
	* 如果订单参数`priceProtect`为true:
		* 达到触发价时，`MARK_PRICE`(标记价格)与`CONTRACT_PRICE`(合约最新价)之间的价差不能超过改symbol触发保护阈值
		* 触发保护阈值请参考接口`GET /fapi/v1/exchangeInfo` 返回内容相应symbol中"triggerProtect"字段

	* `STOP`, `STOP_MARKET` 止损单:
		* 买入: 最新合约价格/标记价格高于等于触发价`stopPrice`
		* 卖出: 最新合约价格/标记价格低于等于触发价`stopPrice`
	* `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 止盈单:
		* 买入: 最新合约价格/标记价格低于等于触发价`stopPrice`
		* 卖出: 最新合约价格/标记价格高于等于触发价`stopPrice`

	* `TRAILING_STOP_MARKET` 跟踪止损单:
		* 买入: 当合约价格/标记价格区间最低价格低于激活价格`activationPrice`,且最新合约价格/标记价高于等于最低价设定回调幅度。
		* 卖出: 当合约价格/标记价格区间最高价格高于激活价格`activationPrice`,且最新合约价格/标记价低于等于最高价设定回调幅度。

* `TRAILING_STOP_MARKET` 跟踪止损单如果遇到报错 ``{"code": -2021, "msg": "Order would immediately trigger."}``    
表示订单不满足以下条件:
	* 买入: 指定的`activationPrice` 必须小于 latest price
	* 卖出: 指定的`activationPrice` 必须大于 latest price

* `newOrderRespType` 如果传 `RESULT`:
	* `MARKET` 订单将直接返回成交结果；
	* 配合使用特殊 `timeInForce` 的 `LIMIT` 订单将直接返回成交或过期拒绝结果。

* `STOP_MARKET`, `TAKE_PROFIT_MARKET` 配合 `closePosition`=`true`:
	* 条件单触发依照上述条件单触发逻辑
	* 条件触发后，平掉当时持有所有多头仓位(若为卖单)或当时持有所有空头仓位(若为买单)
	* 不支持 `quantity` 参数
	* 自带只平仓属性，不支持`reduceOnly`参数
	* 双开模式下,`LONG`方向上不支持`BUY`; `SHORT` 方向上不支持`SELL`


## 测试下单接口 (TRADE)


> **响应:**

```javascript
字段与下单接口一致，但均为无效值
```


``
POST /fapi/v1/order/test (HMAC SHA256)
``

用于测试订单请求，但不会提交到撮合引擎

**权重:**
1

**参数:**

参考 `POST /fapi/v1/order`



## 批量下单 (TRADE)


> **响应:**

```javascript
[
	{
	 	"clientOrderId": "testOrder", // 用户自定义的订单号
	 	"cumQty": "0",
	 	"cumQuote": "0", // 成交金额
	 	"executedQty": "0", // 成交量
	 	"orderId": 22542179, // 系统订单号
	 	"avgPrice": "0.00000",	// 平均成交价
	 	"origQty": "10", // 原始委托数量
	 	"price": "0", // 委托价格
	 	"reduceOnly": false, // 仅减仓
	 	"side": "SELL", // 买卖方向
	 	"positionSide": "SHORT", // 持仓方向
	 	"status": "NEW", // 订单状态
	 	"stopPrice": "0", // 触发价，对`TRAILING_STOP_MARKET`无效
	 	"closePosition": false,   // 是否条件全平仓
	 	"symbol": "BTCUSDT", // 交易对
	 	"timeInForce": "GTC", // 有效方法
	 	"type": "TRAILING_STOP_MARKET", // 订单类型
	 	"origType": "TRAILING_STOP_MARKET",  // 触发前订单类型
	 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	 	"updateTime": 1566818724722, // 更新时间
	 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
	 	"priceProtect": false            // 是否开启条件单触发保护
	},
	{
		"code": -2022, 
		"msg": "ReduceOnly Order is rejected."
	}
]
```

``
POST /fapi/v1/batchOrders  (HMAC SHA256)
``

**权重:**
5

**参数:**


名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
batchOrders |	list<JSON> | 	YES |	订单列表，最多支持5个订单
recvWindow |	LONG |	NO	
timestamp	| LONG | YES	

**其中``batchOrders``应以list of JSON格式填写订单参数**

名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
symbol           | STRING  | YES      | 交易对
side             | ENUM    | YES      | 买卖方向 `SELL`, `BUY`
positionSide     | ENUM	    | NO       | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT`   
type             | ENUM    | YES      | 订单类型 `LIMIT`, `MARKET`, `STOP`, `TAKE_PROFIT`, `STOP_MARKET`, `TAKE_PROFIT_MARKET`, `TRAILING_STOP_MARKET`
reduceOnly       | STRING  | NO       | `true`, `false`; 非双开模式下默认`false`；双开模式下不接受此参数。
quantity         | DECIMAL | YES      | 下单数量
price            | DECIMAL | NO       | 委托价格
newClientOrderId | STRING  | NO       | 用户自定义的订单号，不可以重复出现在挂单中。如空缺系统会自动赋值. 必须满足正则规则 `^[\.A-Z\:/a-z0-9_-]{1,36}$`
stopPrice        | DECIMAL | NO       | 触发价, 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
activationPrice  | DECIMAL | NO       | 追踪止损激活价格，仅`TRAILING_STOP_MARKET` 需要此参数, 默认为下单当前市场价格(支持不同`workingType`)
callbackRate     | DECIMAL | NO       | 追踪止损回调比例，可取值范围[0.1, 4],其中 1代表1% ,仅`TRAILING_STOP_MARKET` 需要此参数
timeInForce      | ENUM    | NO       | 有效方法
workingType      | ENUM    | NO       | stopPrice 触发类型: `MARK_PRICE`(标记价格), `CONTRACT_PRICE`(合约最新价). 默认 `CONTRACT_PRICE`
priceProtect | STRING | NO | 条件单触发保护："TRUE","FALSE", 默认"FALSE". 仅 `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` 需要此参数
newOrderRespType | ENUM    | NO       | "ACK", "RESULT", 默认 "ACK"


* 具体订单条件规则，与普通下单一致
* 批量下单采取并发处理，不保证订单撮合顺序
* 批量下单的返回内容顺序，与订单列表顺序一致

## 期货现货互转 (TRADE)

> **响应:**

```javascript
{
    "tranId": 21841, //交易id
    "status": "SUCCESS" //状态
}
```

``
POST /fapi/v1/asset/wallet/transfer  (HMAC SHA256)
``

**权重:**
5

**参数:**


名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	数量
asset |	STRING | 	YES |	资产
clientTranId |	STRING | 	YES |	交易id 
kindType |	STRING | 	YES |	交易类型
timestamp	| LONG | YES	|	时间戳

* kindType 取值为FUTURE_SPOT(期货转现货),SPOT_FUTURE(现货转期货)


## 查询订单 (USER_DATA)


> **响应:**

```javascript
{
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",					// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1573346959,				// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",						// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", 			// 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020",			// 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",					// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
GET /fapi/v1/order (HMAC SHA256)
``

查询订单状态

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单的最终状态为 `CANCELED` 或者 `EXPIRED`, **并且** 
	* 订单没有任何的成交记录, **并且**
	* 订单生成时间 + 7天 < 当前时间

**权重:**
1

**参数:**

名称        |  类型  | 是否必需 | 描述
----------------- | ------ | -------- | ----
symbol            | STRING | YES      | 交易对
orderId           | LONG   | NO       | 系统订单号
origClientOrderId | STRING | NO       | 用户自定义的订单号
recvWindow        | LONG   | NO       |
timestamp         | LONG   | YES      |

注意:

* 至少需要发送 `orderId` 与 `origClientOrderId`中的一个


## 撤销订单 (TRADE)

> **响应:**

```javascript
{
 	"clientOrderId": "myOrder1", // 用户自定义的订单号
 	"cumQty": "0",
 	"cumQuote": "0", // 成交金额
 	"executedQty": "0", // 成交量
 	"orderId": 283194212, // 系统订单号
 	"origQty": "11", // 原始委托数量
 	"price": "0", // 委托价格
	"reduceOnly": false, // 仅减仓
	"side": "BUY", // 买卖方向
	"positionSide": "SHORT", // 持仓方向
 	"status": "CANCELED", // 订单状态
 	"stopPrice": "9300", // 触发价，对`TRAILING_STOP_MARKET`无效
 	"closePosition": false,   // 是否条件全平仓
 	"symbol": "BTCUSDT", // 交易对
 	"timeInForce": "GTC", // 有效方法
 	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
 	"type": "TRAILING_STOP_MARKET", // 订单类型
 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
 	"updateTime": 1571110484038, // 更新时间
 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
DELETE /fapi/v1/order  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

名称               |  类型   | 是否必需  |        描述
----------------- | ------ | -------- | ------------------
symbol            | STRING | YES      | 交易对
orderId           | LONG   | NO       | 系统订单号
origClientOrderId | STRING | NO       | 用户自定义的订单号
recvWindow        | LONG   | NO       |
timestamp         | LONG   | YES      |

`orderId` 与 `origClientOrderId` 必须至少发送一个


## 撤销全部订单 (TRADE)

> **响应:**

```javascript
{
	"code": "200", 
	"msg": "The operation of cancel all open order is done."
}
```

``
DELETE /fapi/v1/allOpenOrders  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol     | STRING | YES      | 交易对
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 批量撤销订单 (TRADE)

> **响应:**

```javascript
[
	{
	 	"clientOrderId": "myOrder1", // 用户自定义的订单号
	 	"cumQty": "0",
	 	"cumQuote": "0", // 成交金额
	 	"executedQty": "0", // 成交量
	 	"orderId": 283194212, // 系统订单号
	 	"origQty": "11", // 原始委托数量
	 	"price": "0", // 委托价格
		"reduceOnly": false, // 仅减仓
		"side": "BUY", // 买卖方向
		"positionSide": "SHORT", // 持仓方向
	 	"status": "CANCELED", // 订单状态
	 	"stopPrice": "9300", // 触发价，对`TRAILING_STOP_MARKET`无效
	 	"closePosition": false,   // 是否条件全平仓
	 	"symbol": "BTCUSDT", // 交易对
	 	"timeInForce": "GTC", // 有效方法
	 	"origType": "TRAILING_STOP_MARKET", // 触发前订单类型
 		"type": "TRAILING_STOP_MARKET", // 订单类型
	 	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  		"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
	 	"updateTime": 1571110484038, // 更新时间
	 	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
	 	"priceProtect": false            // 是否开启条件单触发保护
	},
	{
		"code": -2011,
		"msg": "Unknown order sent."
	}
]
```

``
DELETE /fapi/v1/batchOrders  (HMAC SHA256)
``

**权重:**
1

**Parameters:**

  名称          |      类型      | 是否必需 |       描述
--------------------- | -------------- | -------- | ----------------
symbol                | STRING         | YES      | 交易对
orderIdList           | LIST\<LONG\>   | NO       | 系统订单号, 最多支持10个订单 <br/> 比如`[1234567,2345678]`
origClientOrderIdList | LIST\<STRING\> | NO       | 用户自定义的订单号, 最多支持10个订单 <br/> 比如`["my_id_1","my_id_2"]` 需要encode双引号。逗号后面没有空格。
recvWindow            | LONG           | NO       |
timestamp             | LONG           | YES      |

`orderIdList` 与 `origClientOrderIdList` 必须至少发送一个，不可同时发送


## 倒计时撤销所有订单 (TRADE)

> **响应:**

```javascript
{
	"symbol": "BTCUSDT", 
	"countdownTime": "100000"
}
```


``
POST /fapi/v1/countdownCancelAll  (HMAC SHA256)
``

**权重:**
10

**Parameters:**

  名称          |      类型      | 是否必需 |       描述
--------------------- | -------------- | -------- | ----------------
symbol | STRING | YES |
countdownTime | LONG | YES | 倒计时。 1000 表示 1 秒； 0 表示取消倒计时撤单功能。
recvWindow | LONG | NO |
timestamp | LONG | YES |

* 该接口可以被用于确保在倒计时结束时撤销指定symbol上的所有挂单。 在使用这个功能时，接口应像心跳一样在倒计时内被反复调用，以便可以取消既有的倒计时并开始新的倒数计时设置。

* 用法示例：
	以30s的间隔重复此接口，每次倒计时countdownTime设置为120000(120s)。   
	如果在120秒内未再次调用此接口，则您指定symbol上的所有挂单都会被自动撤销。   
	如果在120秒内以将countdownTime设置为0，则倒数计时器将终止，自动撤单功能取消。
	
* 系统会**大约每10毫秒**检查一次所有倒计时情况，因此请注意，使用此功能时应考虑足够的冗余。    
我们不建议将倒记时设置得太精确或太小。





## 查询当前挂单 (USER_DATA)

> **响应:**

```javascript

{
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"status": "NEW",					// 订单状态
  	"positionSide": "SHORT", // 持仓方向
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
}
```

``
GET /fapi/v1/openOrder  (HMAC SHA256)
``

请小心使用不带symbol参数的调用

**权重: 1**


**参数:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol | STRING | YES | 交易对
orderId | LONG | NO | 系统订单号
origClientOrderId | STRING | NO | 用户自定义的订单号
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* `orderId` 与 `origClientOrderId` 中的一个为必填参数
* 查询的订单如果已经成交或取消，将返回报错 "Order does not exist."


## 查看当前全部挂单 (USER_DATA)

> **响应:**

```javascript
[
  {
  	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", // 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,   // 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
  }
]
```

``
GET /fapi/v1/openOrders  (HMAC SHA256)
``

请小心使用不带symbol参数的调用

**权重:**
- 带symbol ***1***
- 不带 ***40***

**参数:**

   名称    |  类型  | 是否必需 |  描述
---------- | ------ | -------- | ------
symbol     | STRING | NO      | 交易对
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 不带symbol参数，会返回所有交易对的挂单



## 查询所有订单(包括历史订单) (USER_DATA)


> **响应:**

```javascript
[
  {
   	"avgPrice": "0.00000",				// 平均成交价
  	"clientOrderId": "abc",				// 用户自定义的订单号
  	"cumQuote": "0",						// 成交金额
  	"executedQty": "0",					// 成交量
  	"orderId": 1917641,					// 系统订单号
  	"origQty": "0.40",					// 原始委托数量
  	"origType": "TRAILING_STOP_MARKET",	// 触发前订单类型
  	"price": "0",					// 委托价格
  	"reduceOnly": false,				// 是否仅减仓
  	"side": "BUY",						// 买卖方向
  	"positionSide": "SHORT", // 持仓方向
  	"status": "NEW",					// 订单状态
  	"stopPrice": "9300",					// 触发价，对`TRAILING_STOP_MARKET`无效
  	"closePosition": false,  			// 是否条件全平仓
  	"symbol": "BTCUSDT",				// 交易对
  	"time": 1579276756075,				// 订单时间
  	"timeInForce": "GTC",				// 有效方法
  	"type": "TRAILING_STOP_MARKET",		// 订单类型
  	"activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"priceRate": "0.3",	// 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
  	"updateTime": 1579276756075,		// 更新时间
  	"workingType": "CONTRACT_PRICE", // 条件价格触发类型
 	"priceProtect": false            // 是否开启条件单触发保护
  }
]
```

``
GET /fapi/v1/allOrders (HMAC SHA256)
``

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单的最终状态为 `CANCELED` 或者 `EXPIRED`, **并且** 
	* 订单没有任何的成交记录, **并且**
	* 订单生成时间 + 7天 < 当前时间

**权重:**
5 

**Parameters:**

   名称    |  类型  | 是否必需 |                      描述
---------- | ------ | -------- | -----------------------------------------------
symbol     | STRING | YES      | 交易对
orderId    | LONG   | NO       | 只返回此orderID及之后的订单，缺省返回最近的订单
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值:500 最大值:1000
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 查询时间范围最大不得超过7天
* 默认查询最近7天内的数据



## 账户余额V2 (USER_DATA)

> **响应:**

```javascript
[
 	{
 		"accountAlias": "SgsR",    // 账户唯一识别码
 		"asset": "USDT",		// 资产
 		"balance": "122607.35137903",	// 总余额
 		"crossWalletBalance": "23.72469206", // 全仓余额
  		"crossUnPnl": "0.00000000"  // 全仓持仓未实现盈亏
  		"availableBalance": "23.72469206",       // 下单可用余额
  		"maxWithdrawAmount": "23.72469206",     // 最大可转出余额
  		"marginAvailable": true,    // 是否可用作联合保证金
  		"updateTime": 1617939110373
	}
]
```

``
GET /fapi/v2/balance (HMAC SHA256)
``

**Weight:**
5

**Parameters:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES




## 账户信息V4 (USER_DATA)

> **响应:**

```javascript

{
	"feeTier": 0,  // 手续费等级
 	"canTrade": true,  // 是否可以交易
 	"canDeposit": true,  // 是否可以入金
 	"canWithdraw": true, // 是否可以出金
 	"updateTime": 0,
 	"totalInitialMargin": "0.00000000",  // 但前所需起始保证金总额(存在逐仓请忽略), 仅计算usdt资产
 	"totalMaintMargin": "0.00000000",  // 维持保证金总额, 仅计算usdt资产
 	"totalWalletBalance": "23.72469206",   // 账户总余额, 联保模式下采用BidRate/AskRate来计算余额
 	"totalUnrealizedProfit": "0.00000000",  // 持仓未实现盈亏总额, 仅计算usdt资产
 	"totalMarginBalance": "23.72469206",  // 保证金总余额, 联保模式下采用BidRate/AskRate来计算余额
 	"totalPositionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格), 仅计算usdt资产
 	"totalOpenOrderInitialMargin": "0.00000000",  // 当前挂单所需起始保证金(基于最新标记价格), 仅计算usdt资产
 	"totalCrossWalletBalance": "23.72469206",  // 全仓账户余额, 联保模式下采用BidRate/AskRate来计算余额
 	"totalCrossUnPnl": "0.00000000",	// 全仓持仓未实现盈亏总额, 仅计算usdt资产
 	"availableBalance": "23.72469206",       // 可用余额, 仅计算usdt资产
 	"maxWithdrawAmount": "23.72469206"     // 最大可转出余额, 联保模式下采用BidRate来计算余额
 	"assets": [
 		{
 			"asset": "USDT",	 	//资产
 			"walletBalance": "23.72469206",  //余额
		   	"unrealizedProfit": "0.00000000",  // 未实现盈亏
		   	"marginBalance": "23.72469206",  // 保证金余额
		   	"maintMargin": "0.00000000",	// 维持保证金
		   	"initialMargin": "0.00000000",  // 当前所需起始保证金
		   	"positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
		   	"crossWalletBalance": "23.72469206",  //全仓账户余额
		   	"crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
		   	"availableBalance": "23.72469206",       // 可用余额
		   	"maxWithdrawAmount": "23.72469206",     // 最大可转出余额
		   	"marginAvailable": true,   // 是否可用作联合保证金
		   	"updateTime": 1625474304765  //更新时间
		},
		{
 			"asset": "BUSD",	 	//资产
 			"walletBalance": "103.12345678",  //余额
		   	"unrealizedProfit": "0.00000000",  // 未实现盈亏
		   	"marginBalance": "103.12345678",  // 保证金余额
		   	"maintMargin": "0.00000000",	// 维持保证金
		   	"initialMargin": "0.00000000",  // 当前所需起始保证金
		   	"positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
		   	"crossWalletBalance": "103.12345678",  //全仓账户余额
		   	"crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
		   	"availableBalance": "103.12345678",       // 可用余额
		   	"maxWithdrawAmount": "103.12345678",     // 最大可转出余额
		   	"marginAvailable": true,   // 否可用作联合保证金
		   	"updateTime": 0  // 更新时间
	       }
	],
 	"positions": [  // 头寸，将返回所有市场symbol。
 		//根据用户持仓模式展示持仓方向，即单向模式下只返回BOTH持仓情况，双向模式下只返回 LONG 和 SHORT 持仓情况
 		{
		 	"symbol": "BTCUSDT",  // 交易对
		   	"initialMargin": "0",	// 当前所需起始保证金(基于最新标记价格)
		   	"maintMargin": "0",	//维持保证金
		   	"unrealizedProfit": "0.00000000",  // 持仓未实现盈亏
		   	"positionInitialMargin": "0",  // 持仓所需起始保证金(基于最新标记价格)
		   	"openOrderInitialMargin": "0",  // 当前挂单所需起始保证金(基于最新标记价格)
		   	"leverage": "100",	// 杠杆倍率
		   	"isolated": true,  // 是否是逐仓模式
		   	"entryPrice": "0.00000",  // 持仓成本价
		   	"maxNotional": "250000",  // 当前杠杆下用户可用的最大名义价值
		   	"positionSide": "BOTH",  // 持仓方向
		   	"positionAmt": "0",		 // 持仓数量
		   	"updateTime": 0         // 更新时间 
		}
  	]
}
```


``
GET /fapi/v4/account (HMAC SHA256)
``

**权重:**
5

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO |
timestamp | LONG | YES |




## 调整开仓杠杆 (TRADE)

> **响应:**

```javascript
{
 	"leverage": 21,	// 杠杆倍数
 	"maxNotionalValue": "1000000", // 当前杠杆倍数下允许的最大名义价值
 	"symbol": "BTCUSDT"	// 交易对
}
```

``
POST /fapi/v1/leverage (HMAC SHA256)
``

调整用户在指定symbol合约的开仓杠杆。

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |            描述
---------- | ------ | -------- | ---------------------------
symbol     | STRING | YES      | 交易对
leverage   | INT    | YES      | 目标杠杆倍数：1 到 125 整数
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 变换逐全仓模式 (TRADE)

> **响应:**

```javascript
{
	"code": 200,
	"msg": "success"
}
```

``
POST /fapi/v1/marginType (HMAC SHA256)
``

变换用户在指定symbol合约上的保证金模式：逐仓或全仓。

**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |       描述
---------- | ------ | -------- | -----------------
symbol     | STRING | YES      | 交易对
marginType | ENUM   | YES      | 保证金模式 ISOLATED(逐仓), CROSSED(全仓)
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |


## 调整逐仓保证金 (TRADE)

> **响应:**

```javascript
{
	"amount": 100.0,
  	"code": 200,
  	"msg": "Successfully modify position margin.",
  	"type": 1
}
```

``
POST /fapi/v1/positionMargin (HMAC SHA256)
``

针对逐仓模式下的仓位，调整其逐仓保证金资金。

**权重:**
1

**参数:**

   名称    |  类型   | 是否必需 |                 描述
---------- | ------- | -------- | ------------------------------------
symbol     | STRING  | YES      | 交易对
positionSide| ENUM   | NO		  | 持仓方向，单向持仓模式下非必填，默认且仅可填`BOTH`;在双向持仓模式下必填,且仅可选择 `LONG` 或 `SHORT` 
amount     | DECIMAL | YES      | 保证金资金
type       | INT     | YES      | 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
recvWindow | LONG    | NO       |
timestamp  | LONG    | YES      |

* 只针对逐仓symbol 与 positionSide(如有)


## 逐仓保证金变动历史 (TRADE)

> **响应:**

```javascript
[
	{
		"amount": "23.36332311", // 数量
	  	"asset": "USDT", // 资产
	  	"symbol": "BTCUSDT", // 交易对
	  	"time": 1578047897183, // 时间
	  	"type": 1，	// 调整方向
	  	"positionSide": "BOTH"  // 持仓方向
	},
	{
		"amount": "100",
	  	"asset": "USDT",
	  	"symbol": "BTCUSDT",
	  	"time": 1578047900425,
	  	"type": 1，
	  	"positionSide": "LONG" 
	}
]
```

``
GET /fapi/v1/positionMargin/history (HMAC SHA256)
``



**权重:**
1

**参数:**

   名称    |  类型  | 是否必需 |                 描述
---------- | ------ | -------- | ------------------------------------
symbol     | STRING | YES      | 交易对
type       | INT    | NO       | 调整方向 1: 增加逐仓保证金，2: 减少逐仓保证金
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值: 500
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |




## 用户持仓风险V2 (USER_DATA)

> **响应:**

> 单向持仓模式下：

```javascript
[
  	{
  		"entryPrice": "0.00000", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "0.00000000",	// 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "0", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "0.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "0.00000000", // 持仓未实现盈亏
  		"positionSide": "BOTH", // 持仓方向
  		"updateTime": 1625474304765   // 更新时间
  	}
]
```

> 双向持仓模式下：

```javascript
[
  	{
  		"entryPrice": "6563.66500", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "15517.54150468", // 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "5930.78", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "20.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "2316.83423560" // 持仓未实现盈亏
  		"positionSide": "LONG", // 持仓方向
  		"updateTime": 1625474304765  // 更新时间
  	},
  	{
  		"entryPrice": "0.00000", // 开仓均价
  		"marginType": "isolated", // 逐仓模式或全仓模式
  		"isAutoAddMargin": "false",
  		"isolatedMargin": "5413.95799991", // 逐仓保证金
  		"leverage": "10", // 当前杠杆倍数
  		"liquidationPrice": "7189.95", // 参考强平价格
  		"markPrice": "6679.50671178",	// 当前标记价格
  		"maxNotionalValue": "20000000", // 当前杠杆倍数允许的名义价值上限
  		"positionAmt": "-10.000", // 头寸数量，符号代表多空方向, 正数为多，负数为空
  		"symbol": "BTCUSDT", // 交易对
  		"unRealizedProfit": "-1156.46711780" // 持仓未实现盈亏
  		"positionSide": "SHORT", // 持仓方向
  		"updateTime": 1625474304765  //更新时间
  	}  	
]
```

``
GET /fapi/v2/positionRisk (HMAC SHA256)
``

**权重:**
5

**参数:**

   名称    | 类型 | 是否必需 | 描述
---------- | ---- | -------- | ----
symbol     | STRING | NO     |
recvWindow | LONG | NO       |
timestamp  | LONG | YES      |


**注意**    
请与账户推送信息`ACCOUNT_UPDATE`配合使用，以满足您的及时性和准确性需求。




## 账户成交历史 (USER_DATA)


> **响应:**

```javascript
[
  {
  	"buyer": false,	// 是否是买方
  	"commission": "-0.07819010", // 手续费
  	"commissionAsset": "USDT", // 手续费计价单位
  	"id": 698759,	// 交易ID
  	"maker": false,	// 是否是挂单方
  	"orderId": 25851813, // 订单编号
  	"price": "7819.01",	// 成交价
  	"qty": "0.002",	// 成交量
  	"quoteQty": "15.63802",	// 成交额
  	"realizedPnl": "-0.91539999",	// 实现盈亏
  	"side": "SELL",	// 买卖方向
  	"positionSide": "SHORT",  // 持仓方向
  	"symbol": "BTCUSDT", // 交易对
  	"time": 1569514978020 // 时间
  }
]
```

``
GET /fapi/v1/userTrades  (HMAC SHA256)
``

获取某交易对的成交历史

**权重:**
5

**参数:**

   名称    |  类型  | 是否必需 |                     描述
---------- | ------ | -------- | --------------------------------------------
symbol     | STRING | YES      | 交易对
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
fromId     | LONG   | NO       | 返回该fromId及之后的成交，缺省返回最近的成交
limit      | INT    | NO       | 返回的结果集数量 默认值:500 最大值:1000.
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 如果`startTime` 和 `endTime` 均未发送, 只会返回最近7天的数据。
* startTime 和 endTime 的最大间隔为7天


## 获取账户损益资金流水(USER_DATA)

> **响应:**

```javascript
[
	{
    	"symbol": "", // 交易对，仅针对涉及交易对的资金流
    	"incomeType": "TRANSFER",	// 资金流类型
    	"income": "-0.37500000", // 资金流数量，正数代表流入，负数代表流出
    	"asset": "USDT", // 资产内容
    	"info":"TRANSFER", // 备注信息，取决于流水类型
    	"time": 1570608000000, // 时间
    	"tranId":"9689322392",		// 划转ID
    	"tradeId":""					// 引起流水产生的原始交易ID
	},
	{
   		"symbol": "BTCUSDT",
    	"incomeType": "COMMISSION", 
    	"income": "-0.01000000",
    	"asset": "USDT",
    	"info":"COMMISSION",
    	"time": 1570636800000,
    	"tranId":"9689322392",		
    	"tradeId":"2059192"					
	}
]
```

``
GET /fapi/v1/income (HMAC SHA256)
``

**权重:**
30

**参数:**

   名称    |  类型  | 是否必需 |                                              描述
---------- | ------ | -------- | -----------------------------------------------------------------------------------------------
symbol     | STRING | NO       | 交易对
incomeType | STRING | NO       | 收益类型 "TRANSFER"，"WELCOME_BONUS", "REALIZED_PNL"，"FUNDING_FEE", "COMMISSION", "INSURANCE_CLEAR", and "MARKET_MERCHANT_RETURN_REWARD"
startTime  | LONG   | NO       | 起始时间
endTime    | LONG   | NO       | 结束时间
limit      | INT    | NO       | 返回的结果集数量 默认值:100 最大值:1000
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |

* 如果`startTime` 和 `endTime` 均未发送, 只会返回最近7天的数据。
* 如果`incomeType`没有发送，返回所有类型账户损益资金流水。
* "trandId" 在相同用户的同一种收益流水类型中是唯一的。


## 杠杆分层标准 (USER_DATA)


> **响应:**

```javascript
[
    {
        "symbol": "ETHUSDT",
        "brackets": [
            {
                "bracket": 1,   // 层级
                "initialLeverage": 75,  // 该层允许的最高初始杠杆倍数
                "notionalCap": 10000,  // 该层对应的名义价值上限
                "notionalFloor": 0,  // 该层对应的名义价值下限 
                "maintMarginRatio": 0.0065, // 该层对应的维持保证金率
                "cum":0 // 速算数
            },
        ]
    }
]
```

> **或** (若发送symbol)

```javascript

{
    "symbol": "ETHUSDT",
    "brackets": [
        {
            "bracket": 1,
            "initialLeverage": 75,
            "notionalCap": 10000,
            "notionalFloor": 0,
            "maintMarginRatio": 0.0065,
            "cum":0
        },
    ]
}
```


``
GET /fapi/v1/leverageBracket
``


**权重:** 1

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol	| STRING | NO
recvWindow | LONG   | NO       |
timestamp  | LONG   | YES      |



## 持仓ADL队列估算 (USER_DATA)


> **响应:**

```javascript
[
	{
		"symbol": "ETHUSDT", 
		"adlQuantile": 
			{
				// 对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE", 其中"HEDGE"的存在仅作为标记;如果多空均有持仓的情况下,"LONG"和"SHORT"应返回共同计算后相同的队列分数。
				"LONG": 3,  
				"SHORT": 3, 
				"HEDGE": 0   // HEDGE 仅作为指示出现，请忽略数值
			}
		},
 	{
 		"symbol": "BTCUSDT", 
 		"adlQuantile": 
 			{
 				// 对于单向持仓模式或者是逐仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "BOTH" 分别表示不同持仓方向上持仓的adl队列分数
 				"LONG": 1, 	// 双开模式下多头持仓的ADL队列估算分
 				"SHORT": 2, 	// 双开模式下空头持仓的ADL队列估算分
 				"BOTH": 0		// 单开模式下持仓的ADL队列估算分
 			}
 	}
 ]
```

``
GET /fapi/v1/adlQuantile
``


**权重:** 5

**参数:**

 名称  |  类型  | 是否必需 |  描述
------ | ------ | -------- | ------
symbol	| STRING | NO
recvWindow|LONG|NO| 
timestamp|LONG|YES|

* 每30秒更新数据

* 队列分数0，1，2，3，4，分数越高说明在ADL队列中的位置越靠前

* 对于单向持仓模式或者是逐仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "BOTH" 分别表示不同持仓方向上持仓的adl队列分数

* 对于全仓状态下的双向持仓模式的交易对，会返回 "LONG", "SHORT" 和 "HEDGE", 其中"HEDGE"的存在仅作为标记;其中如果多空均有持仓的情况下,"LONG"和"SHORT"返回共同计算后相同的队列分数。


## 用户强平单历史 (USER_DATA)


> **响应:**

```javascript
[
  {
  	"orderId": 6071832819, 
  	"symbol": "BTCUSDT", 
  	"status": "FILLED", 
  	"clientOrderId": "autoclose-1596107620040000020", 
  	"price": "10871.09", 
  	"avgPrice": "10913.21000", 
  	"origQty": "0.001", 
  	"executedQty": "0.001", 
  	"cumQuote": "10.91321", 
  	"timeInForce": "IOC", 
  	"type": "LIMIT", 
  	"reduceOnly": false, 
  	"closePosition": false, 
  	"side": "SELL", 
  	"positionSide": "BOTH", 
  	"stopPrice": "0", 
  	"workingType": "CONTRACT_PRICE", 
  	"origType": "LIMIT", 
  	"time": 1596107620044, 
  	"updateTime": 1596107620087
  }
  {
   	"orderId": 6072734303, 
   	"symbol": "BTCUSDT", 
   	"status": "FILLED", 
   	"clientOrderId": "adl_autoclose", 
   	"price": "11023.14", 
   	"avgPrice": "10979.82000", 
   	"origQty": "0.001", 
   	"executedQty": "0.001", 
   	"cumQuote": "10.97982", 
   	"timeInForce": "GTC", 
   	"type": "LIMIT", 
   	"reduceOnly": false, 
   	"closePosition": false, 
   	"side": "BUY", 
   	"positionSide": "SHORT", 
   	"stopPrice": "0", 
   	"workingType": "CONTRACT_PRICE", 
   	"origType": "LIMIT", 
   	"time": 1596110725059, 
   	"updateTime": 1596110725071
  }
]
```


``
GET /fapi/v1/forceOrders
``


**权重:** 带symbol 20, 不带symbol 50

**参数:**

  名称      |  类型  | 是否必需 |                   描述
------------- | ------ | -------- | ----------------------------------------
symbol        | STRING | NO       |
autoCloseType | ENUM   | NO       | "LIQUIDATION": 强平单, "ADL": ADL减仓单.
startTime     | LONG   | NO       |
endTime       | LONG   | NO       |
limit         | INT    | NO       | Default 50; max 100.
recvWindow    | LONG   | NO       |
timestamp     | LONG   | YES      |

* 如果没有传 "autoCloseType", 强平单和ADL减仓单都会被返回
* 如果没有传"startTime", 只会返回"endTime"之前7天内的数据



## 用户手续费率 (USER_DATA)

> **响应:**

```javascript
{
	"symbol": "BTCUSDT",
  	"makerCommissionRate": "0.0002",  // 0.02%
  	"takerCommissionRate": "0.0004"   // 0.04%
}
```

``
GET /fapi/v1/commissionRate (HMAC SHA256)
``

**权重:**
20


**参数:**

名称  |  类型  | 是否必需 |  描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES	
recvWindow | LONG | NO	
timestamp | LONG | YES






# Websocket 账户信息推送


* 本篇所列出REST接口的baseurl **https://fapi.asterdex.com**
* 用于订阅账户数据的 `listenKey` 从创建时刻起有效期为60分钟
* 可以通过`PUT`一个`listenKey`延长60分钟有效期
* 可以通过`DELETE`一个 `listenKey` 立即关闭当前数据流，并使该`listenKey` 无效
* 在具有有效`listenKey`的帐户上执行`POST`将返回当前有效的`listenKey`并将其有效期延长60分钟
* 本篇所列出的websocket接口baseurl: **wss://fstream.asterdex.com**
* 订阅账户数据流的stream名称为 **/ws/\<listenKey\>**
* 每个链接有效期不超过24小时，请妥善处理断线重连。
* 账户数据流的消息**不保证**严格时间序; **请使用 E 字段进行排序**
* 考虑到剧烈行情下, RESTful接口可能存在查询延迟，我们强烈建议您优先从Websocket user data stream推送的消息来获取订单，仓位等信息。


## 生成listenKey (USER_STREAM)


> **响应:**

```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

``
POST /fapi/v1/listenKey
``

创建一个新的user data stream，返回值为一个listenKey，即websocket订阅的stream名称。如果该帐户具有有效的`listenKey`，则将返回该`listenKey`并将其有效期延长60分钟。

**权重:**
1

**参数:**

None


## 延长listenKey有效期 (USER_STREAM)


> **响应:**

```javascript
{}
```

``
PUT /fapi/v1/listenKey
``

有效期延长至本次调用后60分钟

**权重:**
1

**参数:**

None



## 关闭listenKey (USER_STREAM)

> **响应:**

```javascript
{}
```

``
DELETE /fapi/v1/listenKey
``

关闭某账户数据流

**权重:**
1

**参数:**

None



## listenKey 过期推送

> **Payload:**

```javascript
{
	'e': 'listenKeyExpired',      // 事件类型
	'E': 1576653824250				// 事件时间
}
```

当前连接使用的有效listenKey过期时，user data stream 将会推送此事件。

**注意:**

* 此事件与websocket连接中断没有必然联系
* 只有正在连接中的有效`listenKey`过期时才会收到此消息
* 收到此消息后user data stream将不再更新，直到用户使用新的有效的`listenKey`




## 追加保证金通知

> **Payload:**

```javascript
{
    "e":"MARGIN_CALL",    	// 事件类型
    "E":1587727187525,		// 事件时间
    "cw":"3.16812045",		// 除去逐仓仓位保证金的钱包余额, 仅在全仓 margin call 情况下推送此字段
    "p":[					// 涉及持仓
      {
        "s":"ETHUSDT",		// symbol
        "ps":"LONG",		// 持仓方向
        "pa":"1.327",		// 仓位
        "mt":"CROSSED",		// 保证金模式
        "iw":"0",			// 若为逐仓，仓位保证金
        "mp":"187.17127",	// 标记价格
        "up":"-1.166074",	// 未实现盈亏
        "mm":"1.614445"		// 持仓需要的维持保证金
      }
    ]
}  
 
```


* 当用户持仓风险过高，会推送此消息。
* 此消息仅作为风险指导信息，不建议用于投资策略。
* 在大波动市场行情下,不排除此消息发出的同时用户仓位已被强平的可能。




## Balance和Position更新推送

> **Payload:**

```javascript
{
  "e": "ACCOUNT_UPDATE",				// 事件类型
  "E": 1564745798939,            		// 事件时间
  "T": 1564745798938 ,           		// 撮合时间
  "a":                          		// 账户更新事件
    {
      "m":"ORDER",						// 事件推出原因 
      "B":[                     		// 余额信息
        {
          "a":"USDT",           		// 资产名称
          "wb":"122624.12345678",    	// 钱包余额
          "cw":"100.12345678",			// 除去逐仓仓位保证金的钱包余额
          "bc":"50.12345678"			// 除去盈亏与交易手续费以外的钱包余额改变量
        },
        {
          "a":"BUSD",           
          "wb":"1.00000000",
          "cw":"0.00000000",         
          "bc":"-49.12345678"
        }
      ],
      "P":[
       {
          "s":"BTCUSDT",          	// 交易对
          "pa":"0",               	// 仓位
          "ep":"0.00000",            // 入仓价格
          "cr":"200",             	// (费前)累计实现损益
          "up":"0",						// 持仓未实现盈亏
          "mt":"isolated",				// 保证金模式
          "iw":"0.00000000",			// 若为逐仓，仓位保证金
          "ps":"BOTH"					// 持仓方向
       }，
       {
        	"s":"BTCUSDT",
        	"pa":"20",
        	"ep":"6563.66500",
        	"cr":"0",
        	"up":"2850.21200",
        	"mt":"isolated",
        	"iw":"13200.70726908",
        	"ps":"LONG"
      	 },
       {
        	"s":"BTCUSDT",
        	"pa":"-10",
        	"ep":"6563.86000",
        	"cr":"-45.04000000",
        	"up":"-1423.15600",
        	"mt":"isolated",
        	"iw":"6570.42511771",
        	"ps":"SHORT"
       }
      ]
    }
}
```

账户更新事件的 event type 固定为 `ACCOUNT_UPDATE`

* 当账户信息有变动时，会推送此事件：
	* 仅当账户信息有变动时(包括资金、仓位、保证金模式等发生变化)，才会推送此事件；
	* 订单状态变化没有引起账户和持仓变化的，不会推送此事件；
	* 每次因持仓变动推送的position 信息，仅包含当前持仓不为0或逐仓仓位保证金不为0的symbol position。

* "FUNDING FEE" 引起的资金余额变化，仅推送简略事件：
	* 当用户某**全仓**持仓发生"FUNDING FEE"时，事件`ACCOUNT_UPDATE`将只会推送相关的用户资产余额信息`B`(仅推送FUNDING FEE 发生相关的资产余额信息)，而不会推送任何持仓信息`P`。
	* 当用户某**逐仓**仓持仓发生"FUNDING FEE"时，事件`ACCOUNT_UPDATE`将只会推送相关的用户资产余额信息`B`(仅推送"FUNDING FEE"所使用的资产余额信息)，和相关的持仓信息`P`(仅推送这笔"FUNDING FEE"发生所在的持仓信息)，其余持仓信息不会被推送。

* 字段"m"代表了事件推出的原因，包含了以下可能类型:
	* DEPOSIT
	* WITHDRAW
	* ORDER
	* FUNDING_FEE
	* WITHDRAW_REJECT
	* ADJUSTMENT
	* INSURANCE_CLEAR
	* ADMIN_DEPOSIT
	* ADMIN_WITHDRAW
	* MARGIN_TRANSFER
	* MARGIN_TYPE_CHANGE
	* ASSET_TRANSFER
	* OPTIONS_PREMIUM_FEE
	* OPTIONS_SETTLE_PROFIT
	* AUTO_EXCHANGE

* 字段"bc"代表了钱包余额的改变量，即 balance change，但注意其不包含仓位盈亏及交易手续费。

## 订单/交易 更新推送

> **Payload:**

```javascript
{
  
  "e":"ORDER_TRADE_UPDATE",			// 事件类型
  "E":1568879465651,				// 事件时间
  "T":1568879465650,				// 撮合时间
  "o":{								
    "s":"BTCUSDT",					// 交易对
    "c":"TEST",						// 客户端自定订单ID
      // 特殊的自定义订单ID:
      // "autoclose-"开头的字符串: 系统强平订单
      // "adl_autoclose": ADL自动减仓订单
    "S":"SELL",						// 订单方向
    "o":"TRAILING_STOP_MARKET",	// 订单类型
    "f":"GTC",						// 有效方式
    "q":"0.001",					// 订单原始数量
    "p":"0",						// 订单原始价格
    "ap":"0",						// 订单平均价格
    "sp":"7103.04",					// 条件订单触发价格，对追踪止损单无效
    "x":"NEW",						// 本次事件的具体执行类型
    "X":"NEW",						// 订单的当前状态
    "i":8886774,					// 订单ID
    "l":"0",						// 订单末次成交量
    "z":"0",						// 订单累计已成交量
    "L":"0",						// 订单末次成交价格
    "N": "USDT",                 	// 手续费资产类型
    "n": "0",                    	// 手续费数量
    "T":1568879465651,				// 成交时间
    "t":0,							// 成交ID
    "b":"0",						// 买单净值
    "a":"9.91",						// 卖单净值
    "m": false,					    // 该成交是作为挂单成交吗？
    "R":false	,				    // 是否是只减仓单
    "wt": "CONTRACT_PRICE",	        // 触发价类型
    "ot": "TRAILING_STOP_MARKET",	// 原始订单类型
    "ps":"LONG"						// 持仓方向
    "cp":false,						// 是否为触发平仓单; 仅在条件订单情况下会推送此字段
    "AP":"7476.89",					// 追踪止损激活价格, 仅在追踪止损单时会推送此字段
    "cr":"5.0",						// 追踪止损回调比例, 仅在追踪止损单时会推送此字段
    "rp":"0"							// 该交易实现盈亏
    
  }
  
}
```


当有新订单创建、订单有新成交或者新的状态变化时会推送此类事件
事件类型统一为 `ORDER_TRADE_UPDATE`

**订单方向**

* BUY 买入
* SELL 卖出

**订单类型**

* MARKET  市价单
* LIMIT	限价单
* STOP		止损单
* TAKE_PROFIT 止盈单
* LIQUIDATION 强平单

**本次事件的具体执行类型**

* NEW
* CANCELED		已撤
* CALCULATED		
* EXPIRED			订单失效
* TRADE			交易
	

**订单状态**

* NEW
* PARTIALLY_FILLED    
* FILLED
* CANCELED
* EXPIRED
* NEW_INSURANCE		风险保障基金(强平)
* NEW_ADL				自动减仓序列(强平)

**有效方式:**

* GTC 
* IOC
* FOK
* GTX
* HIDDEN


## 杠杆倍数等账户配置 更新推送

> **Payload:**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // 事件类型
    "E":1611646737479,		           // 事件时间
    "T":1611646737476,		           // 撮合时间
    "ac":{								
    "s":"BTCUSDT",					   // 交易对
    "l":25						       // 杠杆倍数
     
    }
}  
 
```

> **Or**

```javascript
{
    "e":"ACCOUNT_CONFIG_UPDATE",       // 事件类型
    "E":1611646737479,		           // 事件时间
    "T":1611646737476,		           // 撮合时间
    "ai":{							   // 用户账户配置
    "j":true						   // 联合保证金状态
    }
}  
```

当账户配置发生变化时会推送此类事件类型统一为`ACCOUNT_CONFIG_UPDATE `

当交易对杠杆倍数发生变化时推送消息体会包含对象`ac`表示交易对账户配置，其中`s`代表具体的交易对，`l`代表杠杆倍数

当用户联合保证金状态发生变化时推送消息体会包含对象`ai`表示用户账户配置，其中`j`代表用户联合保证金状态



# 错误代码

> error JSON payload:
 
```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

错误由两部分组成：错误代码和消息。 代码是通用的，但是消息可能会有所不同。


## 10xx - 常规服务器或网络问题
> -1000 UNKNOWN
 * An unknown error occured while processing the request.
 * 处理请求时发生未知错误。

> -1001 DISCONNECTED
 * Internal error; unable to process your request. Please try again.
 * 内部错误; 无法处理您的请求。 请再试一次.

> -1002 UNAUTHORIZED
 * You are not authorized to execute this request.
 * 您无权执行此请求。

> -1003 TOO_MANY_REQUESTS
 * Too many requests queued.
 * 排队的请求过多。
 * Too many requests; please use the websocket for live updates.
 * 请求权重过多； 请使用websocket获取最新更新。
 * Too many requests; current limit is %s requests per minute. Please use the websocket for live updates to avoid polling the API.
 * 请求权重过多； 当前限制为每分钟％s请求权重。 请使用websocket进行实时更新，以避免轮询API。
 * Way too many requests; IP banned until %s. Please use the websocket for live updates to avoid bans.
 * 请求权重过多； IP被禁止，直到％s。 请使用websocket进行实时更新，以免被禁。
 
> -1004 DUPLICATE_IP
 * This IP is already on the white list
 * IP地址已经在白名单

> -1005 NO_SUCH_IP
 * No such IP has been white listed
 * 白名单上没有此IP地址
 
> -1006 UNEXPECTED_RESP
 * An unexpected response was received from the message bus. Execution status unknown.
 * 从消息总线收到意外的响应。执行状态未知。

> -1007 TIMEOUT
 * Timeout waiting for response from backend server. Send status unknown; execution status unknown.
 * 等待后端服务器响应超时。 发送状态未知； 执行状态未知。

> -1014 UNKNOWN_ORDER_COMPOSITION
 * Unsupported order combination.
 * 不支持当前的下单参数组合

> -1015 TOO_MANY_ORDERS
 * Too many new orders.
 * 新订单太多。
 * Too many new orders; current limit is %s orders per %s.
 * 新订单太多； 当前限制为每％s ％s个订单。

> -1016 SERVICE_SHUTTING_DOWN
 * This service is no longer available.
 * 该服务不可用。

> -1020 UNSUPPORTED_OPERATION
 * This operation is not supported.
 * 不支持此操作。

> -1021 INVALID_TIMESTAMP
 * Timestamp for this request is outside of the recvWindow.
  * 此请求的时间戳在recvWindow之外。
 * Timestamp for this request was 1000ms ahead of the server's time.
 * 此请求的时间戳比服务器时间提前1000毫秒。

> -1022 INVALID_SIGNATURE
 * Signature for this request is not valid.
 * 此请求的签名无效。

> -1023 START_TIME_GREATER_THAN_END_TIME
 * Start time is greater than end time.
 * 参数里面的开始时间在结束时间之后


## 11xx - Request issues
> -1100 ILLEGAL_CHARS
 * Illegal characters found in a parameter.
 * 在参数中发现非法字符。
 * Illegal characters found in parameter '%s'; legal range is '%s'.
 * 在参数`％s`中发现非法字符； 合法范围是`％s`。

> -1101 TOO_MANY_PARAMETERS
 * Too many parameters sent for this endpoint.
 * 为此端点发送的参数太多。
 * Too many parameters; expected '%s' and received '%s'.
 * 参数太多；预期为`％s`并收到了`％s`。
 * Duplicate values for a parameter detected.
 * 检测到的参数值重复。

> -1102 MANDATORY_PARAM_EMPTY_OR_MALFORMED
 * A mandatory parameter was not sent, was empty/null, or malformed.
 * 未发送强制性参数，该参数为空/空或格式错误。
 * Mandatory parameter '%s' was not sent, was empty/null, or malformed.
 * 强制参数`％s`未发送，为空/空或格式错误。
 * Param '%s' or '%s' must be sent, but both were empty/null!
 * 必须发送参数`％s`或`％s`，但两者均为空！

> -1103 UNKNOWN_PARAM
 * An unknown parameter was sent.
 * 发送了未知参数。

> -1104 UNREAD_PARAMETERS
 * Not all sent parameters were read.
 * 并非所有发送的参数都被读取。
 * Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.
 * 并非所有发送的参数都被读取； 读取了`％s`参数，但被发送了`％s`。

> -1105 PARAM_EMPTY
 * A parameter was empty.
 * 参数为空。
 * Parameter '%s' was empty.
 * 参数`％s`为空。

> -1106 PARAM_NOT_REQUIRED
 * A parameter was sent when not required.
 * 发送了不需要的参数。
 * Parameter '%s' sent when not required.
 * 发送了不需要参数`％s`。

> -1111 BAD_PRECISION
 * Precision is over the maximum defined for this asset.
 * 精度超过为此资产定义的最大值。

> -1112 NO_DEPTH
 * No orders on book for symbol.
 * 交易对没有挂单。
 
> -1114 TIF_NOT_REQUIRED
 * TimeInForce parameter sent when not required.
 * 发送的`TimeInForce`参数不需要。

> -1115 INVALID_TIF
 * Invalid timeInForce.
 * 无效的`timeInForce`

> -1116 INVALID_ORDER_TYPE
 * Invalid orderType.
 * 无效订单类型。

> -1117 INVALID_SIDE
 * Invalid side.
 * 无效买卖方向。

> -1118 EMPTY_NEW_CL_ORD_ID
 * New client order ID was empty.
 * 新的客户订单ID为空。

> -1119 EMPTY_ORG_CL_ORD_ID
 * Original client order ID was empty.
 * 客户自定义的订单ID为空。

> -1120 BAD_INTERVAL
 * Invalid interval.
 * 无效时间间隔。

> -1121 BAD_SYMBOL
 * Invalid symbol.
 * 无效的交易对。

> -1125 INVALID_LISTEN_KEY
 * This listenKey does not exist.
 * 此`listenKey`不存在。

> -1127 MORE_THAN_XX_HOURS
 * Lookup interval is too big.
 * 查询间隔太大。
 * More than %s hours between startTime and endTime.
 * 从开始时间到结束时间之间超过`％s`小时。

> -1128 OPTIONAL_PARAMS_BAD_COMBO
 * Combination of optional parameters invalid.
 * 可选参数组合无效。

> -1130 INVALID_PARAMETER
 * Invalid data sent for a parameter.
 * 发送的参数为无效数据。
 * Data sent for parameter '%s' is not valid.
 * 发送参数`％s`的数据无效。

> -1136 INVALID_NEW_ORDER_RESP_TYPE
 * Invalid newOrderRespType.
 * 无效的 newOrderRespType。


## 20xx - Processing Issues
> -2010 NEW_ORDER_REJECTED
 * NEW_ORDER_REJECTED
 * 新订单被拒绝

> -2011 CANCEL_REJECTED
 * CANCEL_REJECTED
 * 取消订单被拒绝

> -2013 NO_SUCH_ORDER
 * Order does not exist.
 * 订单不存在。

> -2014 BAD_API_KEY_FMT
 * API-key format invalid.
 * API-key 格式无效。

> -2015 REJECTED_MBX_KEY
 * Invalid API-key, IP, or permissions for action.
 * 无效的API密钥，IP或操作权限。

> -2016 NO_TRADING_WINDOW
 * No trading window could be found for the symbol. Try ticker/24hrs instead.
 * 找不到该交易对的交易窗口。 尝试改为24小时自动报价。

> -2018 BALANCE_NOT_SUFFICIENT
 * Balance is insufficient.
 * 余额不足

> -2019 MARGIN_NOT_SUFFICIEN
 * Margin is insufficient.
 * 杠杆账户余额不足

> -2020 UNABLE_TO_FILL
 * Unable to fill.
 * 无法成交

> -2021 ORDER_WOULD_IMMEDIATELY_TRIGGER
 * Order would immediately trigger.
 * 订单可能被立刻触发

> -2022 REDUCE_ONLY_REJECT
 * ReduceOnly Order is rejected.
 * `ReduceOnly`订单被拒绝

> -2023 USER_IN_LIQUIDATION
 * User in liquidation mode now.
 * 用户正处于被强平模式

> -2024 POSITION_NOT_SUFFICIENT
 * Position is not sufficient.
 * 持仓不足

> -2025 MAX_OPEN_ORDER_EXCEEDED
 * Reach max open order limit.
 * 挂单量达到上限

> -2026 REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED
 * This OrderType is not supported when reduceOnly.
 * 当前订单类型不支持`reduceOnly`

> -2027 MAX_LEVERAGE_RATIO
 * Exceeded the maximum allowable position at current leverage.
 * 挂单或持仓超出当前初始杠杆下的最大值

> -2028 MIN_LEVERAGE_RATIO
 * Leverage is smaller than permitted: insufficient margin balance.
 * 调整初始杠杆过低，导致可用余额不足 

## 40xx - Filters and other Issues
> -4000 INVALID_ORDER_STATUS
 * Invalid order status.
 * 订单状态不正确

> -4001 PRICE_LESS_THAN_ZERO
 * Price less than 0.
 * 价格小于0

> -4002 PRICE_GREATER_THAN_MAX_PRICE
 * Price greater than max price.
 * 价格超过最大值
 
> -4003 QTY_LESS_THAN_ZERO
 * Quantity less than zero.
 * 数量小于0

> -4004 QTY_LESS_THAN_MIN_QTY
 * Quantity less than min quantity.
 * 数量小于最小值
 
> -4005 QTY_GREATER_THAN_MAX_QTY
 * Quantity greater than max quantity.
 * 数量大于最大值

> -4006 STOP_PRICE_LESS_THAN_ZERO
 * Stop price less than zero. 
 * 触发价小于最小值
 
> -4007 STOP_PRICE_GREATER_THAN_MAX_PRICE
 * Stop price greater than max price.
 * 触发价大于最大值

> -4008 TICK_SIZE_LESS_THAN_ZERO
 * Tick size less than zero.
 * 价格精度小于0

> -4009 MAX_PRICE_LESS_THAN_MIN_PRICE
 * Max price less than min price.
 * 最大价格小于最小价格

> -4010 MAX_QTY_LESS_THAN_MIN_QTY
 * Max qty less than min qty.
 * 最大数量小于最小数量

> -4011 STEP_SIZE_LESS_THAN_ZERO
 * Step size less than zero.
 * 步进值小于0

> -4012 MAX_NUM_ORDERS_LESS_THAN_ZERO
 * Max num orders less than zero.
 * 最大订单量小于0

> -4013 PRICE_LESS_THAN_MIN_PRICE
 * Price less than min price.
 * 价格小于最小价格

> -4014 PRICE_NOT_INCREASED_BY_TICK_SIZE
 * Price not increased by tick size.
 * 价格增量不是价格精度的倍数。
 
> -4015 INVALID_CL_ORD_ID_LEN
 * Client order id is not valid.
 * 客户订单ID有误。
 * Client order id length should not be more than 36 chars
 * 客户订单ID长度应该不多于36字符

> -4016 PRICE_HIGHTER_THAN_MULTIPLIER_UP
 * Price is higher than mark price multiplier cap.

> -4017 MULTIPLIER_UP_LESS_THAN_ZERO
 * Multiplier up less than zero.
 * 价格上限小于0

> -4018 MULTIPLIER_DOWN_LESS_THAN_ZERO
 * Multiplier down less than zero.
 * 价格下限小于0

> -4019 COMPOSITE_SCALE_OVERFLOW
 * Composite scale too large.

> -4020 TARGET_STRATEGY_INVALID
 * Target strategy invalid for orderType '%s',reduceOnly '%b'.
 * 目标策略值不适合`%s`订单状态, 只减仓`%b`。

> -4021 INVALID_DEPTH_LIMIT
 * Invalid depth limit.
 * 深度信息的`limit`值不正确。
 * '%s' is not valid depth limit.
 * `%s`不是合理的深度信息的`limit`值。

> -4022 WRONG_MARKET_STATUS
 * market status sent is not valid.
 * 发送的市场状态不正确。
 
> -4023 QTY_NOT_INCREASED_BY_STEP_SIZE
 * Qty not increased by step size.
 * 数量的递增值不是步进值的倍数。

> -4024 PRICE_LOWER_THAN_MULTIPLIER_DOWN
 * Price is lower than mark price multiplier floor.

> -4025 MULTIPLIER_DECIMAL_LESS_THAN_ZERO
 * Multiplier decimal less than zero.

> -4026 COMMISSION_INVALID
 * Commission invalid.
 * 收益值不正确
 * `%s` less than zero.
 * `%s`少于0
 * `%s` absolute value greater than `%s`
 * `%s`绝对值大于`%s`

> -4027 INVALID_ACCOUNT_TYPE
 * Invalid account type.
 * 账户类型不正确。

> -4028 INVALID_LEVERAGE
 * Invalid leverage
 * 杠杆倍数不正确
 * Leverage `%s` is not valid
 * 杠杆`%s`不正确
 * Leverage `%s` already exist with `%s`
 * 杠杆`%s`已经存在于`%s`

> -4029 INVALID_TICK_SIZE_PRECISION
 * Tick size precision is invalid.
 * 价格精度小数点位数不正确。

> -4030 INVALID_STEP_SIZE_PRECISION
 * Step size precision is invalid.
 * 步进值小数点位数不正确。

> -4031 INVALID_WORKING_TYPE
 * Invalid parameter working type
 * 不正确的参数类型
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

> -4032 EXCEED_MAX_CANCEL_ORDER_SIZE
 * Exceed maximum cancel order size.
 * 超过可以取消的最大订单量。
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

> -4033 INSURANCE_ACCOUNT_NOT_FOUND
 * Insurance account not found.
 * 风险保障基金账号没找到。

> -4044 INVALID_BALANCE_TYPE
 * Balance Type is invalid.
 * 余额类型不正确。

> -4045 MAX_STOP_ORDER_EXCEEDED
 * Reach max stop order limit.
 * 达到止损单的上限。

> -4046 NO_NEED_TO_CHANGE_MARGIN_TYPE
 * No need to change margin type.
 * 不需要切换仓位模式。

> -4047 THERE_EXISTS_OPEN_ORDERS
 * Margin type cannot be changed if there exists open orders.
 * 如果有挂单，仓位模式不能切换。

> -4048 THERE_EXISTS_QUANTITY
 * Margin type cannot be changed if there exists position.
 * 如果有仓位，仓位模式不能切换。

> -4049 ADD_ISOLATED_MARGIN_REJECT
 * Add margin only support for isolated position.

> -4050 CROSS_BALANCE_INSUFFICIENT
 * Cross balance insufficient.
 * 全仓余额不足。

> -4051 ISOLATED_BALANCE_INSUFFICIENT
 * Isolated balance insufficient.
 * 逐仓余额不足。

> -4052 NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN
 * No need to change auto add margin.

> -4053 AUTO_ADD_CROSSED_MARGIN_REJECT
 * Auto add margin only support for isolated position.
 * 自动增加保证金只适用于逐仓。

> -4054 ADD_ISOLATED_MARGIN_NO_POSITION_REJECT
 * Cannot add position margin: position is 0.
 * 不能增加逐仓保证金: 持仓为0

> -4055 AMOUNT_MUST_BE_POSITIVE
 * Amount must be positive.
 * 数量必须是正整数

> -4056 INVALID_API_KEY_TYPE
 * Invalid api key type.
 * API key的类型不正确

> -4057 INVALID_RSA_PUBLIC_KEY
 * Invalid api public key
 * API key不正确

> -4058 MAX_PRICE_TOO_LARGE
 * maxPrice and priceDecimal too large,please check.
 * maxPrice和priceDecimal太大，请检查。

> -4059 NO_NEED_TO_CHANGE_POSITION_SIDE
 * No need to change position side.
 * 无需变更仓位方向

> -4060 INVALID_POSITION_SIDE
 * Invalid position side.
 * 仓位方向不正确。

> -4061 POSITION_SIDE_NOT_MATCH
 * Order's position side does not match user's setting.
 * 订单的持仓方向和用户设置不一致。

> -4062 REDUCE_ONLY_CONFLICT
 * Invalid or improper reduceOnly value.
 * 仅减仓的设置不正确。

> -4063 INVALID_OPTIONS_REQUEST_TYPE
 * Invalid options request type
 * 无效的期权请求类型

> -4064 INVALID_OPTIONS_TIME_FRAME
 * Invalid options time frame
 * 无效的期权时间窗口

> -4065 INVALID_OPTIONS_AMOUNT
 * Invalid options amount
 * 无效的期权数量

> -4066 INVALID_OPTIONS_EVENT_TYPE
 * Invalid options event type
 * 无效的期权事件类型

> -4067 POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS
 * Position side cannot be changed if there exists open orders.
 * 如果有挂单，无法修改仓位方向。

> -4068 POSITION_SIDE_CHANGE_EXISTS_QUANTITY
 * Position side cannot be changed if there exists position.
 * 如果有仓位, 无法修改仓位方向。

> -4069 INVALID_OPTIONS_PREMIUM_FEE
 * Invalid options premium fee
 * 无效的期权费

> -4070 INVALID_CL_OPTIONS_ID_LEN
 * Client options id is not valid.
 * 客户的期权ID不合法
 * Client options id length should be less than 32 chars
 * 客户的期权ID长度应该小于32个字符

> -4071 INVALID_OPTIONS_DIRECTION
 * Invalid options direction
 * 期权的方向无效

> -4072 OPTIONS_PREMIUM_NOT_UPDATE
 * premium fee is not updated, reject order
 * 期权费没有更新

> -4073 OPTIONS_PREMIUM_INPUT_LESS_THAN_ZERO
 * input premium fee is less than 0, reject order
 * 输入的期权费小于0

> -4074 OPTIONS_AMOUNT_BIGGER_THAN_UPPER
 * Order amount is bigger than upper boundary or less than 0, reject order

> -4075 OPTIONS_PREMIUM_OUTPUT_ZERO
 * output premium fee is less than 0, reject order

> -4076 OPTIONS_PREMIUM_TOO_DIFF
 * original fee is too much higher than last fee
 * 期权的费用比之前的费用高 

> -4077 OPTIONS_PREMIUM_REACH_LIMIT
 * place order amount has reached to limit, reject order
 * 下单的数量达到上限

> -4078 OPTIONS_COMMON_ERROR
 * options internal error
 * 期权内部系统错误

> -4079 INVALID_OPTIONS_ID
 * invalid options id
 * invalid options id: %s
 * duplicate options id %d for user %d
 * 期权ID无效

> -4080 OPTIONS_USER_NOT_FOUND
 * user not found
 * user not found with id: %s
 * 用户找不到

> -4081 OPTIONS_NOT_FOUND
 * options not found
 * options not found with id: %s
 * 期权找不到

> -4082 INVALID_BATCH_PLACE_ORDER_SIZE
 * Invalid number of batch place orders.
 * Invalid number of batch place orders: %s
 * 批量下单的数量不正确

> -4083 PLACE_BATCH_ORDERS_FAIL
 * Fail to place batch orders.
 * 无法批量下单

> -4084 UPCOMING_METHOD
 * Method is not allowed currently. Upcoming soon.
 * 方法不支持

> -4085 INVALID_NOTIONAL_LIMIT_COEF
 * Invalid notional limit coefficient
 * 期权的有限系数不正确

> -4086 INVALID_PRICE_SPREAD_THRESHOLD
 * Invalid price spread threshold
 * 无效的价差阀值
 
> -4087 REDUCE_ONLY_ORDER_PERMISSION
 * User can only place reduce only order
 * 用户只能下仅减仓订单

> -4088 NO_PLACE_ORDER_PERMISSION
 * User can not place order currently
 * 用户当前不能下单

> -4104 INVALID_CONTRACT_TYPE
 * Invalid contract type
 * 无效的合约类型

> -4114 INVALID_CLIENT_TRAN_ID_LEN
 * clientTranId  is not valid
 * clientTranId不正确
 * Client tran id length should be less than 64 chars
 * 客户的tranId长度应该小于64个字符

> -4115 DUPLICATED_CLIENT_TRAN_ID
 * clientTranId  is duplicated
 *  clientTranId重复
 * Client tran id should be unique within 7 days
 * 客户的tranId应在7天内唯一

> -4118 REDUCE_ONLY_MARGIN_CHECK_FAILED
 * ReduceOnly Order Failed. Please check your existing position and open orders
 * 仅减仓订单失败。请检查现有的持仓和挂单
 
> -4131 MARKET_ORDER_REJECT
 * The counterparty's best price does not meet the PERCENT_PRICE filter limit
 * 交易对手的最高价格未达到PERCENT_PRICE过滤器限制

> -4135 INVALID_ACTIVATION_PRICE
 * Invalid activation price
 * 无效的激活价格

> -4137 QUANTITY_EXISTS_WITH_CLOSE_POSITION
 * Quantity must be zero with closePosition equals true
 * 数量必须为0，当closePosition为true时

> -4138 REDUCE_ONLY_MUST_BE_TRUE
 * Reduce only must be true with closePosition equals true
 * Reduce only 必须为true，当closePosition为true时

> -4139 ORDER_TYPE_CANNOT_BE_MKT
 * Order type can not be market if it's unable to cancel
 * 订单类型不能为市价单如果不能取消

> -4140 INVALID_OPENING_POSITION_STATUS
 * Invalid symbol status for opening position
 * 无效的交易对状态

> -4141 SYMBOL_ALREADY_CLOSED
 * Symbol is closed
 * 交易对已下架

> -4142 STRATEGY_INVALID_TRIGGER_PRICE
 * REJECT: take profit or stop order will be triggered immediately
 * 拒绝：止盈止损单将立即被触发

> -4144 INVALID_PAIR
 * Invalid pair
 * 无效的pair

> -4161 ISOLATED_LEVERAGE_REJECT_WITH_POSITION
 * Leverage reduction is not supported in Isolated Margin Mode with open positions
 * 逐仓仓位模式下无法降低杠杆

> -4164 MIN_NOTIONAL
 * Order's notional must be no smaller than 5.0 (unless you choose reduce only)
 *  订单的名义价值不可以小于5，除了使用reduce only
 * Order's notional must be no smaller than %s (unless you choose reduce only)
 *  订单的名义价值不可以小于`%s`，除了使用reduce only

> -4165 INVALID_TIME_INTERVAL
 * Invalid time interval
 * 无效的间隔
 * Maximum time interval is %s days
 * 最大的时间间隔为 `%s` 天

> -4183 PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP
 * Price is higher than stop price multiplier cap.
 * 止盈止损订单价格不应高于触发价与报价乘数上限的乘积
 * Limit price can't be higher than %s.
 * 止盈止损订单价格不应高于 `%s`

> -4184 PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN
 * Price is lower than stop price multiplier floor.
 * 止盈止损订单价格不应低于触发价与报价乘数下限的乘积
 * Limit price can't be lower than %s.
 * 止盈止损订单价格不应低于 `%s`


---

# aster-finance-spot-api.md

# Spot API Overview

* This document lists the base URL for the API endpoints: [**https://sapi.asterdex.com**](https://sapi.asterdex.com)  
* All API responses are in JSON format.  
* All times and timestamps are in UNIX time, in **milliseconds**.

## API Key settings

* Many endpoints require an API Key to access.  
* When setting the API Key, for security reasons it is recommended to set an IP access whitelist.  
* **Never reveal your API key/secret to anyone.**

If an API Key is accidentally exposed, immediately delete that Key and generate a new one.

### Attention
* TESTUSDT or any other symbols starting with TEST are symbols used for Aster’s INTERNAL TESTING ONLY. Please DO NOT trade on these symbols starting with TEST. Aster does not hold any accountability for loss of funds due to trading on these symbols. However, if you run into issues, you may contact support about this any time, we will try to help you recover your funds.

### HTTP return codes

* HTTP `4XX` status codes are used to indicate errors in the request content, behavior, or format. The problem lies with the requester.  
* HTTP `403` status code indicates a violation of WAF restrictions (Web Application Firewall).  
* HTTP `429` error code indicates a warning that the access frequency limit has been exceeded and the IP is about to be blocked.  
* HTTP `418` indicates that after receiving a 429 you continued to access, so the IP has been blocked.  
* HTTP `5XX` error codes are used to indicate issues on the Aster service side.

### API error codes

* When using the endpoint `/api/v1`, any endpoint may throw exceptions;

The API error codes are returned in the following format:

```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

### Basic information about the endpoint

* Endpoints with the `GET` method must send parameters in the `query string`.  
* For `POST`, `PUT`, and `DELETE` endpoints, parameters can be sent in the `query string` with content type `application/x-www-form-urlencoded` , or in the `request body`.  
* The order of parameters is not required.

---

## Access restrictions

### Basic information on access restrictions

* The `rateLimits` array in `/api/v1/exchangeInfo` contains objects related to REQUEST\_WEIGHT and ORDERS rate limits for trading. These are further defined in the `enum definitions` section under `rateLimitType`.  
* A 429 will be returned when any of the rate limits are violated.

### IP access limits

* Each request will include a header named `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` that contains the used weight of all requests from the current IP.  
* Each endpoint has a corresponding weight, and some endpoints may have different weights depending on their parameters. The more resources an endpoint consumes, the higher its weight will be.  
* Upon receiving a 429, you are responsible for stopping requests and must not abuse the API.  
* **If you continue to violate access limits after receiving a 429, your IP will be banned and you will receive a 418 error code.**  
* Repeated violations of the limits will result in progressively longer bans, **from a minimum of 2 minutes up to a maximum of 3 days**.  
* The `Retry-After` header will be sent with responses bearing 418 or 429, and will give the wait time **in seconds** (if 429\) to avoid the ban, or, if 418, until the ban ends.  
* **Access restrictions are based on IP, not API Key**

You are advised to use WebSocket messages to obtain the corresponding data as much as possible to reduce the load and rate-limit pressure from requests.

### Order rate limits

* Each successful order response will include a `X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)` header containing the number of order limit units currently used by the account.  
* When the number of orders exceeds the limit, you will receive a response with status 429 but without the `Retry-After` header. Please check the order rate limits in `GET api/v1/exchangeInfo` (rateLimitType \= ORDERS) and wait until the ban period ends.  
* Rejected or unsuccessful orders are not guaranteed to include the above header in the response.  
* **Order placement rate limits are counted per account.**

### WebSocket connection limits

* The WebSocket server accepts a maximum of 5 messages per second. Messages include:  
  * PING frame  
  * PONG frame  
  * Messages in JSON format, such as subscribe and unsubscribe.  
* If a user sends messages that exceed the limit, the connection will be terminated. IPs that are repeatedly disconnected may be blocked by the server.  
* A single connection can subscribe to up to **1024** Streams.

---

## API authentication types

* Each API has its own authentication type, which determines what kind of authentication should be performed when accessing it.  
* The authentication type will be indicated next to each endpoint name in this document; if not specifically stated, it defaults to `NONE`.  
* If API keys are required, they should be passed in the HTTP header using the `X-MBX-APIKEY` field.  
* API keys and secret keys are **case-sensitive**.   
* By default, API keys have access to all authenticated routes.

| Authentication type | Description |
| :---- | :---- |
| NONE | APIs that do not require authentication |
| TRADE | A valid API-Key and signature are required |
| USER\_DATA | A valid API-Key and signature are required |
| USER\_STREAM | A valid API-Key is required |
| MARKET\_DATA | A valid API-Key is required |

* The `TRADE` and `USER_DATA` endpoints are signed (SIGNED) endpoints.

---

## SIGNED (TRADE AND USER\_DATA) Endpoint security

* When calling a `SIGNED` endpoint, in addition to the parameters required by the endpoint itself, you must also pass a `signature` parameter in the `query string` or `request body`.  
* The signature uses the `HMAC SHA256` algorithm. The API-Secret corresponding to the API-KEY is used as the key for `HMAC SHA256`, and all other parameters are used as the data for the `HMAC SHA256` operation; the output is the signature.  
* The `signature` is **case-insensitive**.  
* "totalParams" is defined as the "query string" concatenated with the "request body".

### Time synchronization safety

* Signed endpoints must include the `timestamp` parameter, whose value should be the unix timestamp (milliseconds) at the moment the request is sent.  
* When the server receives a request it will check the timestamp; if it was sent more than 5,000 milliseconds earlier, the request will be considered invalid. This time window value can be defined by sending the optional `recvWindow` parameter.

The logical pseudocode is as follows:

```javascript
  if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow)
  {
    // process request
  } 
  else 
  {
    // reject request
  }
```

**About trade timeliness** Internet conditions are not completely stable or reliable, so the latency from your client to Aster's servers will experience jitter. This is why we provide `recvWindow`; if you engage in high-frequency trading and have strict requirements for timeliness, you can adjust `recvWindow` flexibly to meet your needs.

It is recommended to use a recvWindow of under 5 seconds. It must not exceed 60 seconds.

### Example of POST /api/v1/order

Below is an example of placing an order by calling the API using echo, openssl, and curl tools in a Linux bash environment. The apiKey and secretKey are for demonstration only.

| Key | Value |
| :---- | :---- |
| apiKey | 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795 |
| secretKey | fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313 |

| Parameters | Values |
| :---- | :---- |
| symbol | BNBUSDT |
| side | BUY |
| type | LIMIT |
| timeInForce | GTC |
| quantity | 5 |
| price | 1.1 |
| recvWindow | 5000 |
| timestamp | 1756187806000 |

#### Example 1: All parameters are sent through the request body

**Example 1** **HMAC SHA256 signature:**

```shell
    $ echo -n "symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000" | openssl dgst -sha256 -hmac "fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313"
    (stdin)= e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0
```

**curl command:**

```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795" -X POST 'https://sapi.asterdex.com/api/v1/order' -d 'symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000&signature=e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0'
```

* **requestBody:**

symbol=BNBUSDT \&side=BUY \&type=LIMIT \&timeInForce=GTC \&quantity=5 \&price=1.1 \&recvWindow=5000 \&timestamp=1756187806000

#### Example 2: All parameters sent through the query string

**Example 2** **HMAC SHA256 signature:**

```shell
    $ echo -n "symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000" | openssl dgst -sha256 -hmac "fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313"
    (stdin)= e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0 
```

**curl command:**

```shell
    (HMAC SHA256)
   $ curl -H "X-MBX-APIKEY: 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795" -X POST 'https://sapi.asterdex.com/api/v1/order?symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000&signature=e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0'
```

* **queryString:**

symbol=BNBUSDT \&side=BUY \&type=LIMIT \&timeInForce=GTC \&quantity=5 \&price=1.1 \&recvWindow=5000 \&timestamp=1756187806000

---

## Public API parameters

### Terminology

The terminology in this section applies throughout the document. New users are encouraged to read it carefully for better understanding.

* `base asset` refers to the asset being traded in a trading pair, i.e., the asset name written first; for example, in `BTCUSDT`, `BTC` is the `base asset`.  
* `quote asset` refers to the pricing asset of a trading pair, i.e., the asset name written at the latter part; for example, in `BTCUSDT`, `USDT` is the `quote asset`.

### Enumeration definition

**Trading pair status (status):**

* TRADING \- after trade

**Trading pair type:**

* SPOT \- spot

**Order status (status):**

| Status | Description |
| :---- | :---- |
| NEW | Order accepted by the matching engine |
| PARTIALLY\_FILLED | Part of the order was filled |
| FILLED | The order was fully filled |
| CANCELED | The user canceled the order |
| REJECTED | The order was not accepted by the matching engine and was not processed |
| EXPIRED | Order canceled by the trading engine, for example: Limit FOK order not filled, Market order not fully filled, orders canceled during exchange maintenance |

**Order types (orderTypes, type):**

* LIMIT \- Limit Order  
* MARKET \- Market Order  
* STOP \- Limit Stop Order  
* TAKE\_PROFIT \- Limit Take-Profit Order  
* STOP\_MARKET \- Market Stop Order  
* TAKE\_PROFIT\_MARKET \- Market Take-Profit Order

**Order response type (newOrderRespType):**

* ACK  
* RESULT  
* FULL

**Order direction (direction side):**

* BUY \- Buy  
* SELL \- Sell

**Valid types (timeInForce):**

This defines how long an order can remain valid before expiring.

| Status | Description |
| :---- | :---- |
| GTC (Good ‘Til Canceled) | The order remains active until it is fully executed or manually canceled. |
| IOC (Immediate or Cancel) | The order will execute immediately for any amount available. Any unfilled portion is automatically canceled. |
| FOK (Fill or Kill) | The order must be fully executed immediately. If it cannot be filled in full, it is canceled right away. |
| GTX (Good till crossing, Post only) | The post-only limit order will only be placed if it can be added as a maker order and not as a taker order.  |

**K-line interval:**

m (minutes), h (hours), d (days), w (weeks), M (months)

* 1m  
* 3m  
* 5m  
* 15m  
* 30m  
* 1h  
* 2h  
* 4h  
* 6h  
* 8h  
* 12h  
* 1d  
* 3d  
* 1w  
* 1M

**Rate limit type (rateLimitType)**

REQUEST\_WEIGHT

```json
    {
      "rateLimitType": "REQUEST_WEIGHT",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 1200
    }
```

ORDERS

```json
    {
      "rateLimitType": "ORDERS",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 100
    }
```

* REQUEST\_WEIGHT \- The maximum sum of request weights allowed within a unit time  
    
* ORDERS \- Order placement frequency limit per time unit

**Interval restriction (interval)**

* MINUTE \- Minute

---

## Filters

Filters, i.e. Filter, define a set of trading rules. There are two types: filters for trading pairs `symbol filters`, and filters for the entire exchange `exchange filters` (not supported yet)

### Trading pair filters

#### PRICE\_FILTER Price filter

**Format in the /exchangeInfo response:**

```javascript
  {                     
    "minPrice": "556.72",
    "maxPrice": "4529764",
    "filterType": "PRICE_FILTER",
    "tickSize": "0.01"   
  }
```

The `Price Filter` checks the validity of the `price` parameter in an order. It consists of the following three parts:

* `minPrice` defines the minimum allowed value for `price`/`stopPrice`.  
* `maxPrice` defines the maximum allowed value for `price`/`stopPrice`.  
* `tickSize` defines the step interval for `price`/`stopPrice`, meaning the price must equal minPrice plus an integer multiple of tickSize.

Each of the above items can be 0; when 0 it means that item is not constrained.

The logical pseudocode is as follows:

* `price` \>= `minPrice`  
* `price` \<= `maxPrice`  
* (`price`\-`minPrice`) % `tickSize` \== 0

#### PERCENT\_PRICE price amplitude filter

**Format in the /exchangeInfo response:**

```javascript
  {                    
	"multiplierDown": "0.9500",
	"multiplierUp": "1.0500",
	"multiplierDecimal": "4",
	"filterType": "PERCENT_PRICE"
  }
```

The `PERCENT_PRICE` filter defines the valid range of prices based on the index price.

For the "price percentage" to apply, the "price" must meet the following conditions:

* `price` \<=`indexPrice` \*`multiplierUp`  
* `price`\> \=`indexPrice` \*`multiplierDown`

#### LOT\_SIZE order size

**Format in the /exchangeInfo response:**

```javascript
  {
    "stepSize": "0.00100000",
    "filterType": "LOT_SIZE",
    "maxQty": "100000.00000000",
    "minQty": "0.00100000"
  }
```

Lots is an auction term. The `LOT_SIZE` filter validates the `quantity` (i.e., the amount) parameter in orders. It consists of three parts:

* `minQty` indicates the minimum allowed value for `quantity`.  
* `maxQty` denotes the maximum allowed value for `quantity`.  
* `stepSize` denotes the allowed step increment for `quantity`.

The logical pseudocode is as follows:

* `quantity` \>= `minQty`  
* `quantity` \<= `maxQty`  
* (`quantity`\-`minQty`) % `stepSize` \== 0

#### MARKET\_LOT\_SIZE \- Market order size

\***/exchangeInfo response format:**

```javascript
  {
    "stepSize": "0.00100000",
    "filterType": "MARKET_LOT_SIZE"
	"maxQty": "100000.00000000",
	"minQty": "0.00100000"
  }
```

The `MARKET_LOT_SIZE` filter defines the `quantity` (i.e., the "lots" in an auction) rules for `MARKET` orders on a trading pair. There are three parts:

* `minQty` defines the minimum allowed `quantity`.  
* `maxQty` defines the maximum allowed quantity.  
* `stepSize` defines the increments by which the quantity can be increased or decreased.

In order to comply with the `market lot size`, the `quantity` must satisfy the following conditions:

* `quantity` \>= `minQty`  
* `quantity` \<= `maxQty`  
* (`quantity`\-`minQty`) % `stepSize` \== 0

# Market data API

## Test server connectivity

**Response**

```javascript
{}
```

`GET /api/v1/ping`

Test whether the REST API can be reached.

**Weight:** 1

**Parameters:** NONE

## Get server time

**Response**

```javascript
{
  "serverTime": 1499827319559
}
```

`GET /api/v1/time`

Test if the REST API can be reached and retrieve the server time.

**Weight:** 1

**Parameters:** NONE

## Trading specification information

**Response**

```javascript
{
	"timezone": "UTC",
	"serverTime": 1756197279679,
	"rateLimits": [{
			"rateLimitType": "REQUEST_WEIGHT",
			"interval": "MINUTE",
			"intervalNum": 1,
			"limit": 6000
		},
		{
			"rateLimitType": "ORDERS",
			"interval": "MINUTE",
			"intervalNum": 1,
			"limit": 6000
		},
		{
			"rateLimitType": "ORDERS",
			"interval": "SECOND",
			"intervalNum": 10,
			"limit": 300
		}
	],
	"exchangeFilters": [],
	"assets": [{
			"asset": "USD"
		}, {
			"asset": "USDT"
		},
		{
			"asset": "BNB"
		}
	],
	"symbols": [{
		"status": "TRADING",
		"baseAsset": "BNB",
		"quoteAsset": "USDT",
		"pricePrecision": 8,
		"quantityPrecision": 8,
		"baseAssetPrecision": 8,
		"quotePrecision": 8,
		"filters": [{
				"minPrice": "0.01000000",
				"maxPrice": "100000",
				"filterType": "PRICE_FILTER",
				"tickSize": "0.01000000"
			},
			{
				"stepSize": "0.00100000",
				"filterType": "LOT_SIZE",
				"maxQty": "1000",
				"minQty": "1"
			},
			{
				"stepSize": "0.00100000",
				"filterType": "MARKET_LOT_SIZE",
				"maxQty": "900000",
				"minQty": "0.00100000"
			},
			{
				"limit": 200,
				"filterType": "MAX_NUM_ORDERS"
			},
			{
				"minNotional": "5",
				"filterType": "MIN_NOTIONAL"
			},
			{
				"maxNotional": "100",
				"filterType": "MAX_NOTIONAL"
			},
			{
				"maxNotional": "100",
				"minNotional": "5",
				"avgPriceMins": 5,
				"applyMinToMarket": true,
				"filterType": "NOTIONAL",
				"applyMaxToMarket": true
			},
			{
				"multiplierDown": "0",
				"multiplierUp": "5",
				"multiplierDecimal": "0",
				"filterType": "PERCENT_PRICE"
			},
			{
				"bidMultiplierUp": "5",
				"askMultiplierUp": "5",
				"bidMultiplierDown": "0",
				"avgPriceMins": 5,
				"multiplierDecimal": "0",
				"filterType": "PERCENT_PRICE_BY_SIDE",
				"askMultiplierDown": "0"
			}
		],
		"orderTypes": [
			"LIMIT",
			"MARKET",
			"STOP",
			"STOP_MARKET",
			"TAKE_PROFIT",
			"TAKE_PROFIT_MARKET"
		],
		"timeInForce": [
			"GTC",
			"IOC",
			"FOK",
			"GTX"
		],
		"symbol": "BNBUSDT",
		"ocoAllowed": false
	}]
}
```

`GET /api/v1/exchangeInfo`

Retrieve trading rules and trading pair information.

**Weight:** 1

**Parameters:** None

## Depth information

**Response**

```javascript
{
  "lastUpdateId": 1027024,
  "E":1589436922972, //  Message output time
  "T":1589436922959, //  Transaction time
  "bids": [
    [
      "4.00000000", // PRICE
      "431.00000000" // QTY
    ]
  ],
  "asks": [
    [
      "4.00000200",
      "12.00000000"
    ]
  ]
}
```

`GET /api/v1/depth`

**Weight:**

Based on limit adjustments:

| Limitations | Weight |
| :---- | :---- |
| 5, 10, 20, 50 | 2 |
| 100 | 5 |
| 500 | 10 |
| 1000 | 20 |

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| limit | INT | NO | Default 100\. Optional values: \[5, 10, 20, 50, 100, 500, 1000\] |

## Recent trades list

**Response**

```javascript
[
 {
    "id": 657,
    "price": "1.01000000",
    "qty": "5.00000000",
    "baseQty": "4.95049505",
    "time": 1755156533943,
    "isBuyerMaker": false
  }
]
```

`GET /api/v1/trades`

Get recent trades

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| limit | INT | NO | Default 500; maximum 1000 |

## Query historical trades (MARKET\_DATA)

**Response**

```javascript
[
 {
    "id": 1140,
    "price": "1.10000000",
    "qty": "7.27200000",
    "baseQty": "6.61090909",
    "time": 1756094288700,
    "isBuyerMaker": false
 }
]
```

`GET /api/v1/historicalTrades`

Retrieve historical trades

**Weight:** 20

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| limit | INT | NO | Default 500; maximum 1000\. |
| fromId | LONG | NO | Return starting from which trade id. Defaults to returning the most recent trade records. |

## Recent trades (aggregated)

**Response**

```javascript
[
  {
    "a": 26129, // Aggregate tradeId
    "p": "0.01633102", // Price
    "q": "4.70443515", // Quantity
    "f": 27781, // First tradeId
    "l": 27781, // Last tradeId
    "T": 1498793709153, // Timestamp
    "m": true, // Was the buyer the maker?
  }
]
```

`GET /api/v1/aggTrades`

The difference between aggregated trades and individual trades is that trades with the same price, same side, and same time are combined into a single entry.

**Weight:** 20

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| fromId | LONG | NO | Return results starting from the trade ID that includes fromId |
| startTime | LONG | NO | Return results starting from trades after that time |
| endTime | LONG | NO | Return the trade records up to that moment |
| limit | INT | NO | Default 500; maximum 1000\. |

* If you send startTime and endTime, the interval must be less than one hour.  
* If no filter parameters (fromId, startTime, endTime) are sent, the most recent trade records are returned by default

## K-line data

**Response**

```javascript
[
  [
    1499040000000, // Open time
    "0.01634790", // Open
    "0.80000000", // High
    "0.01575800", // Low
    "0.01577100", // Close
    "148976.11427815", // Volume
    1499644799999, // Close time
    "2434.19055334", // Quote asset volume
    308, // Number of trades
    "1756.87402397", // Taker buy base asset volume
    "28.46694368", // Taker buy quote asset volume
  ]
]
```

`GET /api/v1/klines`

Each K-line represents a trading pair. The open time of each K-line can be regarded as a unique ID.

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| interval | ENUM | YES | See the enumeration definition: K-line interval |
| startTime | LONG | NO |  |
| endTime | LONG | NO |  |
| limit | INT | NO | Default 500; maximum 1500\. |

* If startTime and endTime are not sent, the most recent trades are returned by default

## 24h price change

**Response**

```javascript
{
  "symbol": "BTCUSDT",              //symbol
  "priceChange": "-94.99999800",    //price change
  "priceChangePercent": "-95.960",  //price change percent
  "weightedAvgPrice": "0.29628482", //weighted avgPrice
  "prevClosePrice": "3.89000000",   //prev close price
  "lastPrice": "4.00000200",        //last price
  "lastQty": "200.00000000",        //last qty
  "bidPrice": "866.66000000",       //first bid price
  "bidQty": "72.05100000",          //first bid qty
  "askPrice": "866.73000000",       //first ask price
  "askQty": "1.21700000",           //first ask qty
  "openPrice": "99.00000000",       //open price
  "highPrice": "100.00000000",      //high price
  "lowPrice": "0.10000000",         //low price
  "volume": "8913.30000000",        //volume
  "quoteVolume": "15.30000000",     //quote volume
  "openTime": 1499783499040,        //open time
  "closeTime": 1499869899040,       //close time
  "firstId": 28385,   // first id
  "lastId": 28460,    // last id
  "count": 76,         // count
  "baseAsset": "BTC",   //base asset
  "quoteAsset": "USDT"  //quote asset
}
```

`GET /api/v1/ticker/24hr`

24-hour rolling window price change data. Please note that omitting the symbol parameter will return data for all trading pairs; in that case the returned data is an example array for the respective pairs, which is not only large in volume but also has a very high weight.

**Weight:** 1 \= single trading pair; **40** \= When the trading pair parameter is missing (returns all trading pairs)

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | NO |  |

* Please note that omitting the symbol parameter will return data for all trading pairs

## Latest price

**Response**

```javascript
{
   "symbol": "ADAUSDT",
   "price": "1.30000000",
   "time": 1649666690902
}  
```

OR

```javascript
[     
  {
     "symbol": "ADAUSDT",
     "price": "1.30000000",
     "time": 1649666690902
  }
]
```

`GET /api/v1/ticker/price`

Get the latest price for a trading pair

**Weight:** 1 \= Single trading pair; **2** \= No symbol parameter (returns all pairs)

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | NO |  |

* If no trading pair parameter is sent, information for all trading pairs will be returned

## Current best order

**Response**

```javascript
{
  "symbol": "LTCBTC",
  "bidPrice": "4.00000000",
  "bidQty": "431.00000000",
  "askPrice": "4.00000200",
  "askQty": "9.00000000"
  "time": 1589437530011   // Timestamp
}
```

OR

```javascript
[
  {
    "symbol": "LTCBTC",
    "bidPrice": "4.00000000",
    "bidQty": "431.00000000",
    "askPrice": "4.00000200",
    "askQty": "9.00000000",
    "time": 1589437530011   // Timestamp
  }
]
```

`GET /api/v1/ticker/bookTicker`

Return the current best orders (highest bid, lowest ask)

**Weight:** 1 \= Single trading pair; **2** \= No symbol parameter (returns all pairs)

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | NO |  |

* If no trading pair parameter is sent, information for all trading pairs will be returned

## Get symbol fees

**Response**

```javascript
{
   "symbol": "APXUSDT",
   "makerCommissionRate": "0.000200",    
   "takerCommissionRate": "0.000700"
}
```

`GET /api/v1/commissionRate`

Get symbol fees

**Weight:** 20

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| recvWindow | LONG | NO | The assigned value cannot be greater than 60000 |
| timestamp | LONG | YES |  |

# Spot account and trading API

## Place order (TRADE)

**Response ACK:**

```javascript
{
  "symbol": "BTCUSDT", 
  "orderId": 28, 
  "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP", 
  "updateTime": 1507725176595, 
  "price": "0.00000000", 
  "avgPrice": "0.0000000000000000", 
  "origQty": "10.00000000", 
  "cumQty": "0",          
  "executedQty": "10.00000000", 
  "cumQuote": "10.00000000",
  "status": "FILLED",
  "timeInForce": "GTC", 
  "stopPrice": "0",    
  "origType": "LIMIT",  
  "type": "LIMIT", 
  "side": "SELL", 
}
```

`POST /api/v1/order (HMAC SHA256)`

Send order

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| side | ENUM | YES | See enum definition: Order direction |
| type | ENUM | YES | See enumeration definition: Order type |
| timeInForce | ENUM | NO | See enum definition: Time in force |
| quantity | DECIMAL | NO |  |
| quoteOrderQty | DECIMAL | NO |  |
| price | DECIMAL | NO |  |
| newClientOrderId | STRING | NO | Client-customized unique order ID. If not provided, one will be generated automatically. |
| stopPrice | DECIMAL | NO | Only STOP, STOP\_MARKET, TAKE\_PROFIT, TAKE\_PROFIT\_MARKET require this parameter |
| recvWindow | LONG | NO | The value cannot be greater than 60000 |
| timestamp | LONG | YES |  |

Depending on the order `type`, certain parameters are mandatory:

| Type | Mandatory parameters |
| :---- | :---- |
| LIMIT | timeInForce, quantity, price |
| MARKET | quantity or quoteOrderQty |
| STOP and TAKE\_PROFIT | quantity, price, stopPrice |
| STOP\_MARKET and TAKE\_PROFIT\_MARKET | quantity, stopPrice |

Other information:

* Place a `MARKET` `SELL` market order; the user controls the amount of base assets to sell with the market order via `QUANTITY`.  
  * For example, when placing a `MARKET` `SELL` market order on the `BTCUSDT` pair, use `QUANTITY` to let the user specify how much BTC they want to sell.  
* For a `MARKET` `BUY` market order, the user controls how much of the quote asset they want to spend with `quoteOrderQty`; `QUANTITY` will be calculated by the system based on market liquidity. For example, when placing a `MARKET` `BUY` market order on the `BTCUSDT` pair, use `quoteOrderQty` to let the user choose how much USDT to use to buy BTC.  
* A `MARKET` order using `quoteOrderQty` will not violate the `LOT_SIZE` limit rules; the order will be executed as closely as possible to the given `quoteOrderQty`.  
* Unless a previous order has already been filled, orders set with the same `newClientOrderId` will be rejected.

## Cancel order (TRADE)

**Response**

```javascript
{
  "symbol": "BTCUSDT", 
  "orderId": 28, 
  "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP", 
  "updateTime": 1507725176595, 
  "price": "0.00000000", 
  "avgPrice": "0.0000000000000000", 
  "origQty": "10.00000000", 
  "cumQty": "0",            
  "executedQty": "10.00000000", 
  "cumQuote": "10.00000000", 
  "status": "CANCELED", 
  "timeInForce": "GTC", 
  "stopPrice": "0",    
  "origType": "LIMIT",  
  "type": "LIMIT", 
  "side": "SELL",
}
```

`DELETE /api/v1/order (HMAC SHA256)`

Cancel active orders

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| orderId | LONG | NO |  |
| origClientOrderId | STRING | NO |  |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

At least one of `orderId` or `origClientOrderId` must be sent.

## Query order (USER\_DATA)

**Response**

```javascript
{
    "orderId": 38,
    "symbol": "ADA25SLP25",
    "status": "FILLED",
    "clientOrderId": "afMd4GBQyHkHpGWdiy34Li",
    "price": "20",
    "avgPrice": "12.0000000000000000",
    "origQty": "10",
    "executedQty": "10",
    "cumQuote": "120",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "stopPrice": "0",
    "origType": "LIMIT",
    "time": 1649913186270,
    "updateTime": 1649913186297
} 
```

`GET /api/v1/order (HMAC SHA256)`

Query order status

* Please note that orders meeting the following conditions will not be returned:  
  * The final status of the order is `CANCELED` or `EXPIRED`, **and**  
  * The order has no trade records, **and**  
  * Order creation time \+ 7 days \< current time

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| orderId | LONG | NO |  |
| origClientOrderId | STRING | NO |  |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

Note:

* You must send at least one of `orderId` or `origClientOrderId`.

## Current open orders (USER\_DATA)

**Response**

```javascript
[
    {
        "orderId": 349661, 
        "symbol": "BNBUSDT", 
        "status": "NEW", 
        "clientOrderId": "LzypgiMwkf3TQ8wwvLo8RA", 
        "price": "1.10000000", 
        "avgPrice": "0.0000000000000000", 
        "origQty": "5",  
        "executedQty": "0", 
        "cumQuote": "0", 
        "timeInForce": "GTC",
        "type": "LIMIT", 
        "side": "BUY",   
        "stopPrice": "0", 
        "origType": "LIMIT", 
        "time": 1756252940207, 
        "updateTime": 1756252940207, 
    }
]
```

`GET /api/v1/openOrders (HMAC SHA256)`

Retrieve all current open orders for trading pairs. Use calls without a trading pair parameter with caution.

**Weight:**

* With symbol ***1***  
* Without ***40***  

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | NO |  |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

* If the symbol parameter is not provided, it will return the order books for all trading pairs.

## Query all orders (USER\_DATA)

**Response**

```javascript
[
    {
        "orderId": 349661, 
        "symbol": "BNBUSDT", 
        "status": "NEW", 
        "clientOrderId": "LzypgiMwkf3TQ8wwvLo8RA", 
        "price": "1.10000000", 
        "avgPrice": "0.0000000000000000", 
        "origQty": "5",  
        "executedQty": "0", 
        "cumQuote": "0", 
        "timeInForce": "GTC", 
        "type": "LIMIT", 
        "side": "BUY",   
        "stopPrice": "0", 
        "origType": "LIMIT", 
        "time": 1756252940207, 
        "updateTime": 1756252940207, 
    }
]
```

`GET /api/v1/allOrders (HMAC SHA256)`

Retrieve all account orders; active, canceled, or completed.

* Please note that orders meeting the following conditions will not be returned:  
  * Order creation time \+ 7 days \< current time

**Weight:** 5

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | YES |  |
| orderId | LONG | NO |  |
| startTime | LONG | NO |  |
| endTime | LONG | NO |  |
| limit | INT | NO | Default 500; maximum 1000 |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

* The maximum query time range must not exceed 7 days.  
* By default, query data is from the last 7 days.



## Perp-spot transfer (TRADE)

**Response:**

```javascript
{
    "tranId": 21841, //Tran Id
    "status": "SUCCESS" //Status
}
```

`POST /api/v1/asset/wallet/transfer  (HMAC SHA256)`

**Weight:** 5

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| amount | DECIMAL | YES | Quantity |
| asset | STRING | YES | Asset |
| clientTranId | STRING | YES | Transaction ID |
| kindType | STRING | YES | Transaction type |
| timestamp | LONG | YES | Timestamp |

* kindType FUTURE_SPOT(future to spot)/SPOT_FUTURE(spot to future)

## Transfer asset to other address (TRADE)

> **Response:**

```javascript
{
    "tranId": 21841, 
    "status": "SUCCESS" 
}
```

``
POST /api/v1/asset/sendToAddress  (HMAC SHA256)
``

**Weight:**
5

**Parameters:**


Name | Type | Mandatory | Description
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	
asset |	STRING | 	YES |	
toAddress |	STRING | 	YES |	
clientTranId |	STRING | 	NO |	 
recvWindow | LONG | NO | 
timestamp	| LONG | YES	|	

**Note:**
* The target address must be a valid existing account and must not be the same as the sender’s account.
* The toAddress must be an EVM address.
* If clientTranId is provided, its length must be at least 20 characters.


## Get withdraw fee (NONE)
> **Response:**
```javascript
{
  "tokenPrice": 1.00019000,
   "gasCost": 0.5000,
  "gasUsdValue": 0.5
}
```

``
GET /api/v1/aster/withdraw/estimateFee 
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
chainId | STRING | YES | 
asset | STRING | YES | 

**Notes:**
* chainId: 1(ETH),56(BSC),42161(Arbi)
* gasCost: The minimum fee required for a withdrawal

## Withdraw (USER_DATA)
> **Response:**
```javascript
{
  "withdrawId": "1014729574755487744",
  "hash":"0xa6d1e617a3f69211df276fdd8097ac8f12b6ad9c7a49ba75bbb24f002df0ebb"
}
```

``
POST /api/v1/aster/user-withdraw (HMAC SHA256)
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
chainId | STRING | YES | 1(ETH),56(BSC),42161(Arbi)
asset | STRING | YES |
amount | STRING | YES |
fee | STRING | YES |
receiver | STRING | YES |  The address of the current account
nonce | STRING | YES |  The current time in microseconds 
userSignature | STRING | YES | 
recvWindow | LONG | NO | 
timestamp | LONG | YES | 


**Note:** 
* chainId: 1(ETH),56(BSC),42161(Arbi)
* receiver: The address of the current account
* If the futures account balance is insufficient, funds will be transferred from the spot account to the perp account for withdrawal.
* userSignature demo

```shell
const domain = {
    name: 'Aster',
    version: '1',
    chainId: 56,
    verifyingContract: ethers.ZeroAddress,
  }

const currentTime = Date.now() * 1000
 
const types = {
    Action: [
        {name: "type", type: "string"},
        {name: "destination", type: "address"},
        {name: "destination Chain", type: "string"},
        {name: "token", type: "string"},
        {name: "amount", type: "string"},
        {name: "fee", type: "string"},
        {name: "nonce", type: "uint256"},
        {name: "aster chain", type: "string"},
    ],
  }
  const value = {
    'type': 'Withdraw',
    'destination': '0xD9cA6952F1b1349d27f91E4fa6FB8ef67b89F02d',
    'destination Chain': 'BSC',
    'token': 'USDT',
    'amount': '10.123400',
    'fee': '1.234567891',
    'nonce': currentTime,
    'aster chain': 'Mainnet',
  }


const signature = await signer.signTypedData(domain, types, value)
```

## Get User Create Apikey nonce (NONE)

> **Response:**
```javascript

111111

```

``
POST /api/v1/getNonce 
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
address | STRING | YES |
userOperationType | STRING | YES | CREATE_API_KEY
network | STRING | NO | 

**Notes:**
* userOperationType: CREATE_API_KEY
* network: For the Solana network, SOL must be provided; otherwise, this field can be ignored.

## Create Apikey (NONE)

> **Response:**
```javascript
{
    "apiKey": "bb3b24d0a3dec88cb06be58a257e4575cb0b1bb256ad6fd90ae8fd0ee1d102ae",
    "apiSecret": "9fe8f5642ae1961674ea0cb7f957fa99dc8e0421b607c985a963ad2ced90ae1c"
}
```

``
POST /api/v1/createApiKey
``

**Weight:**
1

**Parameters:**

Name | Type | Mandatory | Description
------------ | ------------ | ------------ | ------------
address | STRING | YES |
userOperationType | STRING | YES | CREATE_API_KEY
network | STRING | NO | 
userSignature | STRING | YES | 
apikeyIP | STRING | NO | 
desc | STRING | YES | 
recvWindow | LONG | NO | 
timestamp | LONG | YES | 

**Note:**
* userOperationType: CREATE_API_KEY
* network: For the Solana network, SOL must be provided; otherwise, this field can be ignored.
* desc: The same account cannot be duplicated, and the length must not exceed 20 characters.
* apikeyIP: An array of IP addresses, separated by commas.
* Rate limit: 60 requests per minute per IP.
* userSignature: EVM demo

```shell
const nonce = 111111
const message = 'You are signing into Astherus ${nonce}';
const signature = await signer.signMessage(message);
```

## Account information (USER\_DATA)

**Response**

```javascript
{     
   "feeTier": 0,
   "canTrade": true,
   "canDeposit": true,
   "canWithdraw": true,
   "canBurnAsset": true,
   "updateTime": 0,
   "balances": [
    {
      "asset": "BTC",
      "free": "4723846.89208129",
      "locked": "0.00000000"
    },
    {
      "asset": "LTC",
      "free": "4763368.68006011",
      "locked": "0.00000000"
    }
  ]
}
```

`GET /api/v1/account (HMAC SHA256)`

Retrieve current account information

**Weight:** 5

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

## Account trade history (USER\_DATA)

**Response**

```javascript
[ 
  {
    "symbol": "BNBUSDT",
    "id": 1002,
    "orderId": 266358,
    "side": "BUY",
    "price": "1",
    "qty": "2",
    "quoteQty": "2",
    "commission": "0.00105000",
    "commissionAsset": "BNB",
    "time": 1755656788798,
    "counterpartyId": 19,
    "createUpdateId": null,
    "maker": false,
    "buyer": true
  }
] 
```

`GET /api/v1/userTrades (HMAC SHA256)`

Retrieve the trade history for a specified trading pair of an account

**Weight:** 5

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| symbol | STRING | NO |  |
| orderId | LONG | NO | Must be used together with the parameter symbol |
| startTime | LONG | NO |  |
| endTime | LONG | NO |  |
| fromId | LONG | NO | Starting trade ID. Defaults to fetching the most recent trade. |
| limit | INT | NO | Default 500; maximum 1000 |
| recvWindow | LONG | NO |  |
| timestamp | LONG | YES |  |

* If both `startTime` and `endTime` are not sent, only data from the last 7 days will be returned.  
* The maximum interval between startTime and endTime is 7 days.  
* `fromId` cannot be sent together with `startTime` or `endTime`.      

---

# WebSocket market data feed

* The base URL for all wss endpoints listed in this document is: **wss://sstream.asterdex.com**  
* Streams have either a single raw stream or a combined stream  
* Single raw streams format is \*\*/ws/\*\*  
* The URL format for combined streams is \*\*/stream?streams=//\*\*  
* When subscribing to combined streams, the event payload is wrapped in this format: \*\*{"stream":"","data":}\*\*  
* All trading pairs in stream names are **lowercase**  
* Each link to **sstream.asterdex.com** is valid for no more than 24 hours; please handle reconnections appropriately  
* Every 3 minutes the server sends a ping frame; the client must reply with a pong frame within 10 minutes, otherwise the server will close the connection. The client is allowed to send unpaired pong frames (i.e., the client may send pong frames at a frequency higher than once every 10 minutes to keep the connection alive).

## Real-time subscribe/unsubscribe data streams

* The following messages can be sent via WebSocket to subscribe or unsubscribe to data streams. Examples are shown below.  
* The `id` in the response content is an unsigned integer that serves as the unique identifier for exchanges of information.  
* If the `result` in the response content is `null`, it indicates the request was sent successfully.

### Subscribe to a stream

**Response**

```javascript
{
  "result": null,
  "id": 1
}
```

* **Request** { "method": "SUBSCRIBE", "params": \[ "btcusdt@aggTrade", "btcusdt@depth" \], "id": 1 }

### Unsubscribe from a stream

**Response**

```javascript
{
  "result": null,
  "id": 312
}
```

* **Request** { "method": "UNSUBSCRIBE", "params": \[ "btcusdt@depth" \], "id": 312 }

### Subscribed to the feed

**Response**

```javascript
{
  "result": [
    "btcusdt@aggTrade"
  ],
  "id": 3
}
```

* **Request**  
    
  { "method": "LIST\_SUBSCRIPTIONS", "id": 3 }

### Set properties

Currently, the only configurable property is whether to enable the `combined` ("combined") stream. When connecting using `/ws/` ("raw stream"), the combined property is set to `false`, while connecting using `/stream/` sets the property to `true`.

**Response**

```javascript
{
"result": null,
"id": 5
}
```

* **Request** { "method": "SET\_PROPERTY" "params": \[ "combined", true \], "id": 5 }

### Retrieve properties

**Response**

```javascript
{
  "result": true, // Indicates that combined is set to true.
  "id": 2
}
```

* **Request**  
    
  { "method": "GET\_PROPERTY", "params": \[ "combined" \], "id": 2 }

\#\#\# Error message

| Error message | Description |
| :---- | :---- |
| {"code": 0, "msg": "Unknown property"} | Parameters applied in SET\_PROPERTY or GET\_PROPERTY are invalid |
| {"code": 1, "msg": "Invalid value type: expected Boolean", "id": '%s'} | Only true or false are accepted |
| {"code": 2, "msg": "Invalid request: property name must be a string"} | The provided attribute name is invalid |
| {"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"} | Parameter ID not provided or ID has an invalid type |
| {"code": 2, "msg": "Invalid request: unknown variant %s, expected one of SUBSCRIBE, UNSUBSCRIBE, LIST\_SUBSCRIPTIONS, SET\_PROPERTY, GET\_PROPERTY at line 1 column 28"} | Typo warning, or the provided value is not of the expected type |
| {"code": 2, "msg": "Invalid request: too many parameters"} | Unnecessary parameters were provided in the data |
| {"code": 2, "msg": "Invalid request: property name must be a string"} | Property name not provided |
| {"code": 2, "msg": "Invalid request: missing field method at line 1 column 73"} | Data did not provide method |
| {"code":3,"msg":"Invalid JSON: expected value at line %s column %s"} | JSON syntax error |

## Collection transaction flow

**Payload:**

```javascript
{
  "e": "aggTrade",  // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "a": 12345,       // Aggregate trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "f": 100,         // First trade ID
  "l": 105,         // Last trade ID
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
  "M": true         // Ignore
}
```

The collection transaction stream pushes transaction information and is an aggregation of a single order.

**Stream name:** `<symbol>@aggTrade`

**Update speed:** real-time

## Tick-by-tick trades

**Payload:**

```javascript
{
  "e": "trade",     // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "t": 12345,       // Trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
}
```

**Stream name:** `<symbol>@trade`

Each trade stream pushes the details of every individual trade. A **trade**, also called a transaction, is defined as a match between exactly one taker and one maker.

## K-line streams

**Payload:**

```javascript
{
  "e": "kline",     // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "k": {
    "t": 123400000, // Kline start time
    "T": 123460000, // Kline close time
    "s": "BNBBTC",  // Symbol
    "i": "1m",      // Interval
    "f": 100,       // First trade ID
    "L": 200,       // Last trade ID
    "o": "0.0010",  // Open price
    "c": "0.0020",  // Close price
    "h": "0.0025",  // High price
    "l": "0.0015",  // Low price
    "v": "1000",    // Base asset volume
    "n": 100,       // Number of trades
    "x": false,     // Is this kline closed?
    "q": "1.0000",  // Quote asset volume
    "V": "500",     // Taker buy base asset volume
    "Q": "0.500",   // Taker buy quote asset volume
    "B": "123456"   // Ignore
  }
}
```

The K-line stream pushes per-second updates for the requested type of K-line (the latest candle).

**Stream name:** `<symbol>@kline_<interval>`

**Update speed:** 2000ms

**K-line interval parameter:**

m (minutes), h (hours), d (days), w (weeks), M (months)

* 1m  
* 3m  
* 5m  
* 15m  
* 30m  
* 1h  
* 2h  
* 4h  
* 6h  
* 8h  
* 12h  
* 1d  
* 3d  
* 1w  
* 1M

## Simplified ticker by symbol

**Payload:**

```javascript
  {
    "e": "24hrMiniTicker",  // Event type
    "E": 123456789,         // Event time
    "s": "BNBBTC",          // Symbol
    "c": "0.0025",          // Close price
    "o": "0.0010",          // Open price
    "h": "0.0025",          // High price
    "l": "0.0010",          // Low price
    "v": "10000",           // Total traded base asset volume
    "q": "18"               // Total traded quote asset volume
  }
```

Refreshed simplified 24-hour ticker information by symbol

**Stream name:** `<symbol>@miniTicker`

**Update speed:** 1000ms

## Compact tickers for all symbols in the entire market

**Payload:**

```javascript
[
  {
    // Same as <symbol>@miniTicker payload
  }
]
```

Same as above, but pushes all trading pairs. Note that only updated tickers will be pushed.

**Stream name:** \!miniTicker@arr

**Update speed:** 1000ms

## Full ticker per symbol

**Payload:**

```javascript
{
  "e": "24hrTicker",  // Event type
  "E": 123456789,     // Event time
  "s": "BNBBTC",      // Symbol
  "p": "0.0015",      // Price change
  "P": "250.00",      // Price change percent
  "w": "0.0018",      // Weighted average price
  "c": "0.0025",      // Last price
  "Q": "10",          // Last quantity
  "o": "0.0010",      // Open price
  "h": "0.0025",      // High price
  "l": "0.0010",      // Low price
  "v": "10000",       // Total traded base asset volume
  "q": "18",          // Total traded quote asset volume
  "O": 0,             // Statistics open time
  "C": 86400000,      // Statistics close time
  "F": 0,             // First trade ID
  "L": 18150,         // Last trade Id
  "n": 18151          // Total number of trades
}
```

Pushes per-second tag statistics for a single trading pair over a rolling 24-hour window.

**Stream name:** `<symbol>@ticker`

**Update speed:** 1000ms

## Complete ticker for all trading pairs on the entire market

**Payload:**

```javascript
[
  {
    // Same as <symbol>@ticker payload
  }
]
```

Pushes the full 24-hour refreshed ticker information for all trading pairs across the entire market. Note that tickers without updates will not be pushed.

**Stream name:** `!ticker@arr`

**Update speed:** 1000ms

## Best order book information by symbol

**Payload:**

```javascript
{
  "u":400900217,     // order book updateId
  "s":"BNBUSDT",     // symbol
  "b":"25.35190000", // best bid price
  "B":"31.21000000", // best bid qty
  "a":"25.36520000", // best ask price
  "A":"40.66000000"  // best ask qty
}
```

Real-time push of best order book information for the specified trading pair

**Stream name:** `<symbol>@bookTicker`

**Update speed:** Real-time

## Best order book information across the entire market

**Payload:**

```javascript
{
  // 同 <symbol>@bookTicker payload
}
```

Real-time push of the best order information for all trading pairs

**Stream name:** `!bookTicker`

**Update speed:** Real-time

## Limited depth information

**Payload:**

```javascript
{ 
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 100,           // First update ID in event
  "u": 120,           // Final update ID in event
  "pu": 99,          // Final update Id in last stream(ie `u` in last stream) 
  "bids": [             // Bids to be updated
    [
      "0.0024",         // Price level to be updated
      "10"              // Quantity
    ]
  ],
  "asks": [             // Asks to be updated
    [
      "0.0026",         // Price level to be updated
      "100"             // Quantity
    ]
  ]
} 
```

Limited depth information pushed every second or every 100 milliseconds. Levels indicate how many levels of bid/ask information, optional 5/10/20 levels.

**Stream names:** `<symbol>@depth<levels>` or `<symbol>@depth<levels>@100ms`.

**Update speed:** 1000ms or 100ms

## Incremental depth information

**Payload:**

```javascript
{
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 100,           // First update ID in event
  "u": 120,           // Final update ID in event
  "pu": 99,          // Final update Id in last stream(ie `u` in last stream)
  "b": [              // Bids to be updated
    [
      "5.4",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "5.6",       // Price level to be updated
      "100"          // Quantity
    ]
  ]
}   
```

Pushes the changed parts of the orderbook (if any) every second or every 100 milliseconds

**Stream name:** `<symbol>@depth` or `<symbol>@depth@100ms`

**Update speed:** 1000ms or 100ms

## How to correctly maintain a local copy of an order book

1. Subscribe to **wss://sstream.asterdex.com/ws/bnbbtc@depth**  
2. Start caching the received updates. For the same price level, later updates overwrite earlier ones.  
3. Fetch the REST endpoint [**https://sapi.asterdex.com/api/v1/depth?symbol=BNBBTC\&limit=1000**](https://sapi.asterdex.com/api/v1/depth?symbol=BNBBTC&limit=1000) to obtain a 1000-level depth snapshot  
4. Discard from the currently cached messages those with `u` \<= the `lastUpdateId` obtained in step 3 (drop older, expired information)  
5. Apply the depth snapshot to your local order book copy, and resume updating the local copy from the first WebSocket event whose `U` \<= `lastUpdateId`\+1 **and** `u` \>= `lastUpdateId`\+1  
6. Each new event’s `U` should equal exactly the previous event’s `u`\+1; otherwise packets may have been lost \- restart initialization from step 3  
7. The order quantity in each event represents the current order quantity at that price as an **absolute value**, not a relative change  
8. If the order quantity at a given price is 0, it means the orders at that price have been canceled or filled, and that price level should be removed

# WebSocket account information push

* The base URL for the API endpoints listed in this document is: [**https://sapi.asterdex.com**](https://sapi.asterdex.com)  
* The `listenKey` used to subscribe to account data is valid for 60 minutes from the time of creation  
* You can extend the 60-minute validity of a `listenKey` by sending a `PUT` request  
* You can immediately close the current data stream and invalidate the `listenKey` by sending a `DELETE` for a `listenKey`  
* Sending a `POST` on an account with a valid `listenKey` will return the currently valid `listenKey` and extend its validity by 60 minutes  
* The WebSocket interface baseurl: **wss://sstream.asterdex.com**  
* The stream name for subscribing to the user account data stream is \*\*/ws/\*\*  
* Each connection is valid for no more than 24 hours; please handle disconnections and reconnections appropriately

## Listen Key (spot account)

### Generate Listen Key (USER\_STREAM)

**Response**

```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

`POST /api/v1/listenKey`

Start a new data stream. The data stream will be closed after 60 minutes unless a keepalive is sent. If the account already has a valid `listenKey`, that `listenKey` will be returned and its validity extended by 60 minutes.

**Weight:** 1

**Parameters:** NONE

### Extend Listen Key validity period (USER\_STREAM)

**Response**

```javascript
{}
```

`PUT /api/v1/listenKey`

Validity extended to 60 minutes after this call. It is recommended to send a ping every 30 minutes.

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| listenKey | STRING | YES |  |

### Close Listen Key (USER\_STREAM)

**Response**

```javascript
{}
```

`DELETE /api/v1/listenKey`

Close user data stream

**Weight:** 1

**Parameters:**

| Name | Type | Is it required? | Description |
| :---- | :---- | :---- | :---- |
| listenKey | STRING | YES |  |

## Payload: ACCOUNT\_UPDATE

An `outboundAccountPosition` event is sent whenever an account balance changes; it contains the assets that may have changed due to the event that generated the balance update.

**Payload**

```javascript
{
  "B":[  //Balance
    {
      "a":"SLP25",   //Asset
      "f":"10282.42029415",   //Free
      "l":"653.00000001"   //Locked
    },
    {
      "a":"ADA25",
      "f":"9916.96229880",
      "l":"34.00510000"
    }
  ],
  "e":"outboundAccountPosition",   //Event type
  "T":1649926447190,   //Time of last account update
  "E":1649926447205   //Event Time
  "m":"WITHDRAW" // Event reason type
}
```

## Payload: Order Update

Orders are updated via the `executionReport` event

**Payload**

```javascript
{
 "s":"ADA25SLP25",   // symbol
  "c":"Xzh0gnxT41PStbwqOtXnjD",  // client order id
  "S":"SELL",   // order direction
  "o":"LIMIT",   // order type
  "f":"GTC",   // Time in force
  "q":"10.001000",   // Order quantity
  "p":"19.1000000000",   // Order price
  "ap":"19.0999999955550656",  //average price
  "P":"0",  //stop price
  "x":"TRADE",   // Current execution type
  "X":"PARTIALLY_FILLED",   // Current order status
  "i":27,   // Order ID
  "l":"1",    // Last executed quantity   
  "z":"8.999000",   // Cumulative filled quantity
  "L":"19.1000000000",   // Last executed price
  "n":"0.00382000",   // Commission amount
  "N":"SLP25",   // Commission asset
  "T":1649926447190,   //Trasanction Time
  "t":18,   // transaction id
  "m":true,   // is this trade the maker side?
  "ot":"LIMIT", //original order type
  "O":0,   // Order creation time
  "Z":"171.88089996",   // Cumulative quote asset transacted quantity
  "Y":"19.1000000000000000",   // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
  "Q":"0",   // Quote Order Qty
  "e":"executionReport",   // event
  "E":1649926447209  // event time
}  
```

**Execution type:**

* NEW \- New Order  
* CANCELED \- Order canceled  
* REJECTED \- New order was rejected  
* TRADE \- Order had a new fill  
* EXPIRED \- Order expired (based on the order's Time In Force parameter)

\#错误代码

error JSON payload:

```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

Errors consist of two parts: an error code and a message. The code is standardized, but the message may vary.

## 10xx \- General server or network issues

### \-1000 UNKNOWN

* An unknown error occurred while processing the request.

### \-1001 DISCONNECTED

* Internal error; unable to process your request. Please try again.

### \-1002 UNAUTHORIZED

* You are not authorized to execute this request.

### \-1003 TOO\_MANY\_REQUESTS

* Too many requests queued.  
* Too many requests; please use the WebSocket for live updates.  
* Too many requests; current limit is %s requests per minute. Please use the WebSocket for live updates to avoid polling the API.  
* Too many request weights; IP banned until %s. Please use the WebSocket for live updates to avoid bans.

### \-1004 DUPLICATE\_IP

* This IP is already on the white list.

### \-1005 NO\_SUCH\_IP

* No such IP has been whitelisted.

### \-1006 UNEXPECTED\_RESP

* An unexpected response was received from the message bus. Execution status unknown.

### \-1007 TIMEOUT

* Timeout waiting for response from backend server. Send status unknown; execution status unknown.

### \-1014 UNKNOWN\_ORDER\_COMPOSITION

* The current order parameter combination is not supported.

### \-1015 TOO\_MANY\_ORDERS

* Too many new orders.  
* Too many new orders; the current limit is %s orders per %s.

### \-1016 SERVICE\_SHUTTING\_DOWN

* This service is no longer available.

### \-1020 UNSUPPORTED\_OPERATION

* This operation is not supported.

### \-1021 INVALID\_TIMESTAMP

* Timestamp for this request is outside of the recvWindow.  
* The timestamp for this request was 1000ms ahead of the server's time.

### \-1022 INVALID\_SIGNATURE

* The signature for this request is invalid.

### \-1023 START\_TIME\_GREATER\_THAN\_END\_TIME

* The start time in the parameters is after the end time.

## 11xx \- Request issues

### \-1100 ILLEGAL\_CHARS

* Illegal characters found in a parameter.  
* Illegal characters found in parameter %s; legal range is %s.

### \-1101 TOO\_MANY\_PARAMETERS

* Too many parameters sent for this endpoint.  
* Too many parameters; expected %s and received %s.  
* Duplicate values for a parameter detected.

### \-1102 MANDATORY\_PARAM\_EMPTY\_OR\_MALFORMED

* A mandatory parameter was not sent, was empty/null, or malformed.  
* Mandatory parameter %s was not sent, was empty/null, or malformed.  
* Param %s or %s must be sent, but both were empty/null.

### \-1103 UNKNOWN\_PARAM

* An unknown parameter was sent.

### \-1104 UNREAD\_PARAMETERS

* Not all sent parameters were read.  
* Not all sent parameters were read; read %s parameter(s) but %s parameter(s) were sent.

### \-1105 PARAM\_EMPTY

* A parameter was empty.  
* Parameter %s was empty.

### \-1106 PARAM\_NOT\_REQUIRED

* A parameter was sent when not required. 

### \-1111 BAD\_PRECISION 

* The precision exceeds the maximum defined for this asset.

### \-1112 NO\_DEPTH

* No open orders for the trading pair.

### \-1114 TIF\_NOT\_REQUIRED

* TimeInForce parameter sent when not required.

### \-1115 INVALID\_TIF

* Invalid timeInForce.

### \-1116 INVALID\_ORDER\_TYPE

* Invalid orderType.

### \-1117 INVALID\_SIDE

* Invalid order side.

### \-1118 EMPTY\_NEW\_CL\_ORD\_ID

* New client order ID was empty.

### \-1119 EMPTY\_ORG\_CL\_ORD\_ID

* The client’s custom order ID is empty.

### \-1120 BAD\_INTERVAL

* Invalid time interval.

### \-1121 BAD\_SYMBOL

* Invalid trading pair.

### \-1125 INVALID\_LISTEN\_KEY

* This listenKey does not exist.

### \-1127 MORE\_THAN\_XX\_HOURS

* The query interval is too large.  
* More than %s hours between startTime and endTime.

### \-1128 OPTIONAL\_PARAMS\_BAD\_COMBO 

* Combination of optional parameters invalid. 

### \-1130 INVALID\_PARAMETER 

* The parameter sent contains invalid data.  
* Data sent for parameter %s is not valid. 

### \-1136 INVALID\_NEW\_ORDER\_RESP\_TYPE 

* Invalid newOrderRespType. 

## 20xx \- Processing Issues 

### \-2010 NEW\_ORDER\_REJECTED 

* New order rejected.

### \-2011 CANCEL\_REJECTED

* Order cancellation rejected.

### \-2013 NO\_SUCH\_ORDER

* Order does not exist.

### \-2014 BAD\_API\_KEY\_FMT

* API-key format invalid.

### \-2015 REJECTED\_MBX\_KEY

* Invalid API key, IP, or permissions for action.

### \-2016 NO\_TRADING\_WINDOW

* No trading window could be found for the symbol. Try ticker/24hrs instead.

### \-2018 BALANCE\_NOT\_SUFFICIENT

* Balance is insufficient.

### \-2020 UNABLE\_TO\_FILL

* Unable to fill.

### \-2021 ORDER\_WOULD\_IMMEDIATELY\_TRIGGER

* Order would immediately trigger.

### \-2022 REDUCE\_ONLY\_REJECT

* ReduceOnly Order is rejected.

### \-2024 POSITION\_NOT\_SUFFICIENT

* Position is not sufficient.

### \-2025 MAX\_OPEN\_ORDER\_EXCEEDED

* Reached max open order limit.

### \-2026 REDUCE\_ONLY\_ORDER\_TYPE\_NOT\_SUPPORTED

* This OrderType is not supported when reduceOnly.

## 40xx \- Filters and other Issues

### \-4000 INVALID\_ORDER\_STATUS

* Invalid order status.

### \-4001 PRICE\_LESS\_THAN\_ZERO

* Price less than 0\.

### \-4002 PRICE\_GREATER\_THAN\_MAX\_PRICE

* Price greater than max price.

### \-4003 QTY\_LESS\_THAN\_ZERO

* Quantity less than zero.

### \-4004 QTY\_LESS\_THAN\_MIN\_QTY

* Quantity less than minimum quantity.

### \-4005 QTY\_GREATER\_THAN\_MAX\_QTY

* Quantity greater than maximum quantity.

### \-4006 STOP\_PRICE\_LESS\_THAN\_ZERO

* Stop price less than zero.

### \-4007 STOP\_PRICE\_GREATER\_THAN\_MAX\_PRICE

* Stop price greater than max price.

### \-4008 TICK\_SIZE\_LESS\_THAN\_ZERO

* Tick size less than zero.

### \-4009 MAX\_PRICE\_LESS\_THAN\_MIN\_PRICE

* Max price less than min price.

### \-4010 MAX\_QTY\_LESS\_THAN\_MIN\_QTY

* Maximum quantity less than minimum quantity.

### \-4011 STEP\_SIZE\_LESS\_THAN\_ZERO

* Step size less than zero.

### \-4012 MAX\_NUM\_ORDERS\_LESS\_THAN\_ZERO

* Maximum order quantity less than 0\.

### \-4013 PRICE\_LESS\_THAN\_MIN\_PRICE

* Price less than minimum price.

### \-4014 PRICE\_NOT\_INCREASED\_BY\_TICK\_SIZE

* Price not increased by tick size.

### \-4015 INVALID\_CL\_ORD\_ID\_LEN

* Client order ID is not valid.  
* Client order ID length should not be more than 36 characters.

### \-4016 PRICE\_HIGHTER\_THAN\_MULTIPLIER\_UP

* Price is higher than mark price multiplier cap.

### \-4017 MULTIPLIER\_UP\_LESS\_THAN\_ZERO

* Multiplier up less than zero.

### \-4018 MULTIPLIER\_DOWN\_LESS\_THAN\_ZERO

* Multiplier down less than zero.

### \-4019 COMPOSITE\_SCALE\_OVERFLOW

* Composite scale too large.

### \-4020 TARGET\_STRATEGY\_INVALID

* Target strategy invalid for orderType %s, reduceOnly %b'

### \-4021 INVALID\_DEPTH\_LIMIT

* Invalid depth limit.  
* %s is not a valid depth limit.

### \-4022 WRONG\_MARKET\_STATUS

* Market status sent is not valid.

### \-4023 QTY\_NOT\_INCREASED\_BY\_STEP\_SIZE

* The increment of the quantity is not a multiple of the step size.

### \-4024 PRICE\_LOWER\_THAN\_MULTIPLIER\_DOWN

* Price is lower than mark price multiplier floor.

### \-4025 MULTIPLIER\_DECIMAL\_LESS\_THAN\_ZERO

* Multiplier decimal less than zero.

### \-4026 COMMISSION\_INVALID

* Commission invalid.  
* Incorrect profit value.  
* `%s` less than zero.  
* `%s` absolute value greater than `%s`.

### \-4027 INVALID\_ACCOUNT\_TYPE

* Invalid account type.

### \-4029 INVALID\_TICK\_SIZE\_PRECISION

* Tick size precision is invalid.  
* Price decimal precision is incorrect.

### \-4030 INVALID\_STEP\_SIZE\_PRECISION

* The number of decimal places for the step size is incorrect.

### \-4031 INVALID\_WORKING\_TYPE

* Invalid parameter working type: `%s`

### \-4032 EXCEED\_MAX\_CANCEL\_ORDER\_SIZE

* Exceeds the maximum order quantity that can be canceled.  
* Invalid parameter working type: `%s`

### \-4044 INVALID\_BALANCE\_TYPE

* The balance type is incorrect.

### \-4045 MAX\_STOP\_ORDER\_EXCEEDED

* Reached the stop-loss order limit.

### \-4055 AMOUNT\_MUST\_BE\_POSITIVE

* The quantity must be a positive integer.

### \-4056 INVALID\_API\_KEY\_TYPE

* The API key type is invalid.

### \-4057 INVALID\_RSA\_PUBLIC\_KEY

* The API key is invalid.

### \-4058 MAX\_PRICE\_TOO\_LARGE

* maxPrice and priceDecimal too large, please check.

### \-4060 INVALID\_POSITION\_SIDE

* Invalid position side.

### \-4061 POSITION\_SIDE\_NOT\_MATCH

* The order's position direction does not match the user’s settings.

### \-4062 REDUCE\_ONLY\_CONFLICT

* Invalid or improper reduceOnly value.

### \-4084 UPCOMING\_METHOD

* Method is not allowed currently. Coming soon.

### \-4086 INVALID\_PRICE\_SPREAD\_THRESHOLD

* Invalid price spread threshold.

### \-4087 REDUCE\_ONLY\_ORDER\_PERMISSION

* Users can only place reduce-only orders.

### \-4088 NO\_PLACE\_ORDER\_PERMISSION

* User cannot place orders currently.

### \-4114 INVALID\_CLIENT\_TRAN\_ID\_LEN

* clientTranId is not valid.  
* The customer's tranId length should be less than 64 characters.

### \-4115 DUPLICATED\_CLIENT\_TRAN\_ID

* clientTranId is duplicated.  
* The client's tranId should be unique within 7 days.

### \-4118 REDUCE\_ONLY\_MARGIN\_CHECK\_FAILED

* ReduceOnly Order failed. Please check your existing position and open orders

### \-4131 MARKET\_ORDER\_REJECT

* The counterparty's best price does not meet the PERCENT\_PRICE filter limit.

### \-4135 INVALID\_ACTIVATION\_PRICE

* Invalid activation price.

### \-4137 QUANTITY\_EXISTS\_WITH\_CLOSE\_POSITION

* Quantity must be zero when closePosition is true.

### \-4138 REDUCE\_ONLY\_MUST\_BE\_TRUE

* Reduce only must be true when closePosition is true.

### \-4139 ORDER\_TYPE\_CANNOT\_BE\_MKT

* Order type cannot be a market order if it cannot be canceled.

### \-4140 INVALID\_OPENING\_POSITION\_STATUS

* Invalid symbol status for opening position.

### \-4141 SYMBOL\_ALREADY\_CLOSED

* Trading pair has been delisted.

### \-4142 STRATEGY\_INVALID\_TRIGGER\_PRICE

* Rejected: Take Profit or Stop order would be triggered immediately.

### \-4164 MIN\_NOTIONAL

* Order notional must be at least 5.0 (unless you select Reduce Only)  
* Order notional must be no smaller than %s (unless you choose Reduce Only)

### \-4165 INVALID\_TIME\_INTERVAL

* Invalid time interval  
* Maximum time interval is %s days

### \-4183 PRICE\_HIGHTER\_THAN\_STOP\_MULTIPLIER\_UP

* Limit price cannot be higher than the cap of %s.  
* Take-Profit/Stop-Loss price cannot be higher than the cap of %s.

### \-4184 PRICE\_LOWER\_THAN\_STOP\_MULTIPLIER\_DOWN

* Price is below the stop price limit.  
* Take-Profit/Stop-Loss price must be above the trigger price × multiplier floor.  
* Order price (limit or TP/SL) can’t be below %s.


---

# aster-finance-spot-api_CN.md

# 基本信息
## API 基本信息
* 本篇列出接口的baseurl: **https://sapi.asterdex.com**
* 所有接口的响应都是 JSON 格式。
* 所有时间、时间戳均为UNIX时间，单位为**毫秒**。

## API Key 设置
* 很多接口需要API Key才可以访问.
* 设置API Key的同时，为了安全，建议设置IP访问白名单.
* **永远不要把你的API key/secret告诉给任何人**

<aside class="warning">
如果不小心泄露了API key，请立刻删除此Key, 并可以另外生产新的Key.
</aside>

### 注意事项
* TESTUSDT 或任何其他以 TEST 开头的交易对仅用于 Aster 的内部测试。请不要在这些以 TEST 开头的交易品种上进行交易。Aster 对因交易这些交易对而造成的资金损失不承担任何责任。但是，如果您遇到问题，您可以随时联系支持人员，我们将尽力帮助您收回资金。


### HTTP 返回代码
* HTTP `4XX` 错误码用于指示错误的请求内容、行为、格式。问题在于请求者。
* HTTP `403` 错误码表示违反WAF限制(Web应用程序防火墙)。
* HTTP `429` 错误码表示警告访问频次超限，即将被封IP。
* HTTP `418` 表示收到429后继续访问，IP已经被封禁。
* HTTP `5XX` 错误码用于指示Aster服务侧的问题。    

### 接口错误代码
* 使用接口 `/api/v1`, 每个接口都有可能抛出异常; 
> API的错误代码返回形式如下:
```javascript
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

### 接口的基本信息

* `GET` 方法的接口, 参数必须在 `query string`中发送。
* `POST`, `PUT`, 和 `DELETE` 方法的接口,参数可以在内容形式为`application/x-www-form-urlencoded`的 `query string` 中发送，也可以在 `request body` 中发送。 
* 对参数的顺序不做要求。

---
## 访问限制
### 访问限制基本信息

* 在 `/api/v1/exchangeInfo` `rateLimits` 数组中包含与交易的有关REQUEST_WEIGHT和ORDERS速率限制相关的对象。这些在 `限制种类 (rateLimitType)` 下的 `枚举定义` 部分中进一步定义。
* 违反任何一个速率限制时，将返回429。

### IP 访问限制
* 每个请求的回报中包含一个`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)`的头，其中包含当前IP所有请求的已使用权重。
* 每一个接口均有一个相应的权重(weight)，有的接口根据参数不同可能拥有不同的权重。越消耗资源的接口权重就会越大。
* 收到429时，您有责任停止发送请求，不得滥用API。
* **收到429后仍然继续违反访问限制，会被封禁IP，并收到418错误码**
* 频繁违反限制，封禁时间会逐渐延长，**从最短2分钟到最长3天**。
* `Retry-After`的头会与带有418或429的响应发送，并且会给出**以秒为单位**的等待时长(如果是429)以防止禁令，或者如果是418，直到禁令结束。
* **访问限制是基于IP的，而不是API Key**

<aside class="notice">
建议您尽可能多地使用websocket消息获取相应数据，以减少请求带来的访问限制压力。
</aside>


### 下单频率限制
* 每个成功的下单回报将包含一个`X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)`的头，其中包含当前账户已用的下单限制数量。
* 当下单数超过限制时，会收到带有429但不含`Retry-After`头的响应。请检查 `GET api/v1/exchangeInfo` 的下单频率限制 (rateLimitType = ORDERS) 并等待封禁时间结束。
* 被拒绝或不成功的下单并不保证回报中包含以上头内容。
* **下单频率限制是基于每个账户计数的。**

### WEB SOCKET 连接限制

* Websocket服务器每秒最多接受5个消息。消息包括:
	* PING帧
	* PONG帧
	* JSON格式的消息, 比如订阅, 断开订阅.
* 如果用户发送的消息超过限制，连接会被断开连接。反复被断开连接的IP有可能被服务器屏蔽。
* 单个连接最多可以订阅 **1024** 个Streams。


---
## 接口鉴权类型
* 每个接口都有自己的鉴权类型，鉴权类型决定了访问时应当进行何种鉴权。
* 鉴权类型会在本文档中各个接口名称旁声明，如果没有特殊声明即默认为 `NONE`。
* 如果需要 API-keys，应当在HTTP头中以 `X-MBX-APIKEY`字段传递。
* API-keys 与 secret-keys **是大小写敏感的**。
* 默认 API-keys 可访问所有鉴权路径.

鉴权类型 | 描述
------------ | ------------
NONE | 不需要鉴权的接口
TRADE | 需要有效的 API-Key 和签名
USER_DATA | 需要有效的 API-Key 和签名
USER_STREAM | 需要有效的 API-Key
MARKET_DATA | 需要有效的 API-Key


* `TRADE` 和`USER_DATA` 接口是 签名(SIGNED)接口.

---
## SIGNED (TRADE AND USER_DATA) Endpoint security
* 调用`SIGNED` 接口时，除了接口本身所需的参数外，还需要在`query string` 或 `request body`中传递 `signature`, 即签名参数。
* 签名使用`HMAC SHA256`算法. API-KEY所对应的API-Secret作为 `HMAC SHA256` 的密钥，其他所有参数作为`HMAC SHA256`的操作对象，得到的输出即为签名。
* `签名` **大小写不敏感**.
* "totalParams"定义为与"request body"串联的"query string"。

### 时间同步安全
* 签名接口均需要传递 `timestamp`参数，其值应当是请求发送时刻的unix时间戳(毫秒)。
* 服务器收到请求时会判断请求中的时间戳，如果是5000毫秒之前发出的，则请求会被认为无效。这个时间空窗值可以通过发送可选参数 `recvWindow`来定义。

> 逻辑伪代码如下:
```javascript
  if (timestamp < (serverTime + 1000) && (serverTime - timestamp) <= recvWindow)
  {
    // process request
  } 
  else 
  {
    // reject request
  }
```

**关于交易时效性** 互联网状况并不完全稳定可靠,因此你的程序本地到Aster服务器的时延会有抖动。这是我们设置`recvWindow`的目的所在，如果你从事高频交易，对交易时效性有较高的要求，可以灵活设置`recvWindow`以达到你的要求。

<aside class="notice">
推荐使用5秒以下的 recvWindow! 最多不能超过 60秒!
</aside>

### POST /api/v1/order 的示例
以下是在linux bash环境下使用 echo openssl 和curl工具实现的一个调用接口下单的示例 apikey、secret仅供示范

Key | Value
------------ | ------------
apiKey | 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795
secretKey | fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313


参数 | 取值
------------ | ------------
symbol | BNBUSDT
side | BUY
type | LIMIT
timeInForce | GTC
quantity | 5
price | 1.1
recvWindow | 5000
timestamp | 1756187806000


#### 示例 1: 所有参数通过 request body 发送

> **Example 1**
> **HMAC SHA256 signature:**
```shell
    $ echo -n "symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000" | openssl dgst -sha256 -hmac "fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313"
    (stdin)= e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0
```


> **curl command:**
```shell
    (HMAC SHA256)
    $ curl -H "X-MBX-APIKEY: 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795" -X POST 'https://sapi.asterdex.com/api/v1/order' -d 'symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000&signature=e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0'
```

* **requestBody:** 

symbol=BNBUSDT   
&side=BUY   
&type=LIMIT   
&timeInForce=GTC   
&quantity=5   
&price=1.1   
&recvWindow=5000   
&timestamp=1756187806000


#### 示例 2: 所有参数通过 query string 发送

> **Example 2**
> **HMAC SHA256 signature:**
```shell
    $ echo -n "symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000" | openssl dgst -sha256 -hmac "fdde510a2b71fa43a43bff3e3cf7819c8c66df34633d338050f4f59664b3b313"
    (stdin)= e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0 
```
> **curl command:**
```shell
    (HMAC SHA256)
   $ curl -H "X-MBX-APIKEY: 4452d7e2ed4da80b74105e02d06328c71a34488c9fdd60a5a0900d42d584b795" -X POST 'https://sapi.asterdex.com/api/v1/order?symbol=BNBUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=5&price=1.1&recvWindow=5000&timestamp=1756187806000&signature=e09169bf6c02ec4b29fa1bdc3a967f92c8c6cfcde0551ba1d477b2d3cf4c51b0'
```
* **queryString:**  

symbol=BNBUSDT   
&side=BUY   
&type=LIMIT   
&timeInForce=GTC   
&quantity=5   
&price=1.1   
&recvWindow=5000   
&timestamp=1756187806000

---

## 公开 API 参数
### 术语

这里的术语适用于全部文档，建议特别是新手熟读，也便于理解。

* `base asset` 指一个交易对的交易对象，即写在靠前部分的资产名, 比如`BTCUSDT`, `BTC`是`base asset`。
* `quote asset` 指一个交易对的定价资产，即写在靠后部分的资产名, 比如`BTCUSDT`, `USDT`是`quote asset`。

### 枚举定义
**交易对状态 (状态 status):**

* TRADING 交易中


**交易对类型:**

* SPOT 现货

**订单状态 (状态 status):**

状态 | 描述
-----------| --------------
`NEW` | 订单被交易引擎接受
`PARTIALLY_FILLED`| 部分订单被成交
`FILLED` | 订单完全成交
`CANCELED` | 用户撤销了订单
`REJECTED`       | 订单没有被交易引擎接受，也没被处理
`EXPIRED` | 订单被交易引擎取消, 比如 <br/>LIMIT FOK 订单没有成交<br/>市价单没有完全成交<br/>交易所维护期间被取消的订单


**订单类型 (orderTypes, type):**

* LIMIT 限价单
* MARKET 市价单
* STOP 限价止损单
* TAKE_PROFIT 限价止盈单
* STOP_MARKET 市价止损单
* TAKE_PROFIT_MARKET 市价止盈单

**订单返回类型 (newOrderRespType):**

* ACK
* RESULT
* FULL

**订单方向 (方向 side):**

* BUY 买入
* SELL 卖出

**有效方式 (timeInForce):**

这里定义了订单多久能够失效

Status | Description
-----------| --------------
`GTC` | 成交为止 <br> 订单会一直有效，直到被成交或者取消。
`IOC` | 无法立即成交的部分就撤销 <br> 订单在失效前会尽量多的成交。
`FOK` | 无法全部立即成交就撤销 <br> 如果无法全部成交，订单会失效。
`GTX` | 直到挂单成交 <br> 限价只挂单。

**K线间隔:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M

**限制种类 (rateLimitType)**

> REQUEST_WEIGHT
```json
    {
      "rateLimitType": "REQUEST_WEIGHT",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 1200
    }
```

> ORDERS
```json
    {
      "rateLimitType": "ORDERS",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 100
    }
```


* REQUEST_WEIGHT 单位时间请求权重之和上限

* ORDERS 单位时间下单次数限制


**限制间隔 (interval)**

* MINUTE 分

---
## 过滤器 
过滤器，即Filter，定义了一系列交易规则。
共有两类，分别是针对交易对的过滤器`symbol filters`，和针对整个交易所的过滤器`exchange filters`(暂不支持)

### 交易对过滤器  

#### PRICE_FILTER 价格过滤器

> **/exchangeInfo 响应中的格式:**
```javascript
  {                     
    "minPrice": "556.72",
    "maxPrice": "4529764",
    "filterType": "PRICE_FILTER",
    "tickSize": "0.01"   
  }
```

`价格过滤器` 用于检测订单中 `price` 参数的合法性。包含以下三个部分:

* `minPrice` 定义了 `price`/`stopPrice` 允许的最小值。
* `maxPrice` 定义了 `price`/`stopPrice` 允许的最大值。
* `tickSize` 定义了 `price`/`stopPrice` 的步进间隔，即price必须等于minPrice+(tickSize的整数倍)

以上每一项均可为0，为0时代表这一项不再做限制。

逻辑伪代码如下:

* `price` >= `minPrice` 
* `price` <= `maxPrice`
* (`price`-`minPrice`) % `tickSize` == 0


#### PERCENT_PRICE 价格振幅过滤器

> **/exchangeInfo 响应中的格式:**
```javascript
  {                    
	"multiplierDown": "0.9500",
	"multiplierUp": "1.0500",
	"multiplierDecimal": "4",
	"filterType": "PERCENT_PRICE"
  }
```

`PERCENT_PRICE`过滤器基于指数价格来定义价格的有效范围。   

为了通过"价格百分比"，"价格"必须符合以下条件：

* `price` <=`indexPrice` *`multiplierUp`
* `price`> =`indexPrice` *`multiplierDown`


#### LOT_SIZE 订单尺寸

> **/exchangeInfo 响应中的格式:**
```javascript
  {
    "stepSize": "0.00100000",
    "filterType": "LOT_SIZE",
    "maxQty": "100000.00000000",
    "minQty": "0.00100000"
  }
```

Lots是拍卖术语，`LOT_SIZE` 过滤器对订单中的 `quantity` 也就是数量参数进行合法性检查。包含三个部分:

* `minQty` 表示 `quantity` 允许的最小值。
* `maxQty` 表示 `quantity` 允许的最大值。
* `stepSize` 表示 `quantity` 允许的步进值。

逻辑伪代码如下:

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0




#### MARKET_LOT_SIZE 市价订单尺寸

> ***/exchangeInfo 响应中的格式:**
```javascript
  {
    "stepSize": "0.00100000",
    "filterType": "MARKET_LOT_SIZE"
	"maxQty": "100000.00000000",
	"minQty": "0.00100000"
  }
```


`MARKET_LOT_SIZE`过滤器为交易对上的`MARKET`订单定义了`数量`(即拍卖中的"手数")规则。 共有3部分：

* `minQty`定义了允许的最小`quantity`。
* `maxQty`定义了允许的最大数量。
* `stepSize`定义了可以增加/减少数量的间隔。

为了通过`market lot size`，`quantity`必须满足以下条件：

* `quantity` >= `minQty`
* `quantity` <= `maxQty`
* (`quantity`-`minQty`) % `stepSize` == 0










# 行情接口
## 测试服务器连通性
> **响应**
```javascript
{}
```
``
GET /api/v1/ping
``

测试能否联通 Rest API。

**权重:**
1

**参数:**
NONE


## 获取服务器时间
> **响应**
```javascript
{
  "serverTime": 1499827319559
}
```
``
GET /api/v1/time
``

测试能否联通 Rest API 并获取服务器时间。

**权重:**
1

**参数:**
NONE


## 交易规范信息

> **响应** 

```javascript    
{
	"timezone": "UTC",
	"serverTime": 1756197279679,
	"rateLimits": [{
			"rateLimitType": "REQUEST_WEIGHT",
			"interval": "MINUTE",
			"intervalNum": 1,
			"limit": 6000
		},
		{
			"rateLimitType": "ORDERS",
			"interval": "MINUTE",
			"intervalNum": 1,
			"limit": 6000
		},
		{
			"rateLimitType": "ORDERS",
			"interval": "SECOND",
			"intervalNum": 10,
			"limit": 300
		}
	],
	"exchangeFilters": [],
	"assets": [{
			"asset": "USD"
		}, {
			"asset": "USDT"
		},
		{
			"asset": "BNB"
		}
	],
	"symbols": [{
		"status": "TRADING",
		"baseAsset": "BNB",
		"quoteAsset": "USDT",
		"pricePrecision": 8,
		"quantityPrecision": 8,
		"baseAssetPrecision": 8,
		"quotePrecision": 8,
		"filters": [{
				"minPrice": "0.01000000",
				"maxPrice": "100000",
				"filterType": "PRICE_FILTER",
				"tickSize": "0.01000000"
			},
			{
				"stepSize": "0.00100000",
				"filterType": "LOT_SIZE",
				"maxQty": "1000",
				"minQty": "1"
			},
			{
				"stepSize": "0.00100000",
				"filterType": "MARKET_LOT_SIZE",
				"maxQty": "900000",
				"minQty": "0.00100000"
			},
			{
				"limit": 200,
				"filterType": "MAX_NUM_ORDERS"
			},
			{
				"minNotional": "5",
				"filterType": "MIN_NOTIONAL"
			},
			{
				"maxNotional": "100",
				"filterType": "MAX_NOTIONAL"
			},
			{
				"maxNotional": "100",
				"minNotional": "5",
				"avgPriceMins": 5,
				"applyMinToMarket": true,
				"filterType": "NOTIONAL",
				"applyMaxToMarket": true
			},
			{
				"multiplierDown": "0",
				"multiplierUp": "5",
				"multiplierDecimal": "0",
				"filterType": "PERCENT_PRICE"
			},
			{
				"bidMultiplierUp": "5",
				"askMultiplierUp": "5",
				"bidMultiplierDown": "0",
				"avgPriceMins": 5,
				"multiplierDecimal": "0",
				"filterType": "PERCENT_PRICE_BY_SIDE",
				"askMultiplierDown": "0"
			}
		],
		"orderTypes": [
			"LIMIT",
			"MARKET",
			"STOP",
			"STOP_MARKET",
			"TAKE_PROFIT",
			"TAKE_PROFIT_MARKET"
		],
		"timeInForce": [
			"GTC",
			"IOC",
			"FOK",
			"GTX"
		],
		"symbol": "BNBUSDT",
		"ocoAllowed": false
	}]
}
```

``
GET /api/v1/exchangeInfo
``

获取交易规则和交易对信息。

**权重:**
1

**参数:**
无


## 深度信息

> **响应**

```javascript
{
  "lastUpdateId": 1027024,
  "E":1589436922972, // 消息时间
  "T":1589436922959, // 撮合引擎时间
  "bids": [
    [
      "4.00000000", // 价位
      "431.00000000" // 挂单量
    ]
  ],
  "asks": [
    [
      "4.00000200",
      "12.00000000"
    ]
  ]
}
```
``
GET /api/v1/depth
``

**权重:**

基于限制调整:

限制 | 权重
------------ | ------------
5, 10, 20, 50 | 2
100 | 5
500 | 10
1000 | 20

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | 默认 100. 可选值:[5, 10, 20, 50, 100, 500, 1000]


## 近期成交列表

> **响应**

```javascript
[
 {
    "id": 657,
    "price": "1.01000000",
    "qty": "5.00000000",
    "baseQty": "4.95049505",
    "time": 1755156533943,
    "isBuyerMaker": false
  }
]
```
``
GET /api/v1/trades
``

获取近期成交

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | 默认 500；最大1000


## 查询历史成交 (MARKET_DATA)

> **响应**

```javascript
[
 {
    "id": 1140,
    "price": "1.10000000",
    "qty": "7.27200000",
    "baseQty": "6.61090909",
    "time": 1756094288700,
    "isBuyerMaker": false
 }
]
```
``
GET /api/v1/historicalTrades
``

获取历史成交。

**权重:**
20

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
limit | INT | NO | 默认 500; 最大值 1000.
fromId | LONG | NO | 从哪一条成交id开始返回. 缺省返回最近的成交记录。


## 近期成交(归集)

> **响应**

```javascript
[
  {
    "a": 26129, // 归集成交ID
    "p": "0.01633102", // 成交价
    "q": "4.70443515", // 成交量
    "f": 27781, // 被归集的首个成交ID
    "l": 27781, // 被归集的末个成交ID
    "T": 1498793709153, // 成交时间
    "m": true, // 是否为主动卖出单
  }
]
```
``
GET /api/v1/aggTrades
``

归集交易与逐笔交易的区别在于，同一价格、同一方向、同一时间的trade会被聚合为一条

**权重:**
20

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
fromId | LONG | NO | 从包含fromId的成交id开始返回结果
startTime | LONG | NO | 从该时刻之后的成交记录开始返回结果
endTime | LONG | NO | 返回该时刻为止的成交记录
limit | INT | NO | 默认 500; 最大 1000.
* 如果发送startTime和endTime，间隔必须小于一小时。
* 如果没有发送任何筛选参数(fromId, startTime,endTime)，默认返回最近的成交记录


## K线数据

> **响应**
```javascript
[
  [
    1499040000000, // 开盘时间
    "0.01634790", // 开盘价
    "0.80000000", // 最高价
    "0.01575800", // 最低价
    "0.01577100", // 收盘价(当前K线未结束的即为最新价)
    "148976.11427815", // 成交量
    1499644799999, // 收盘时间
    "2434.19055334", // 成交额
    308, // 成交笔数
    "1756.87402397", // 主动买入成交量
    "28.46694368", // 主动买入成交额
  ]
]
```
``
GET /api/v1/klines
``

每根K线代表一个交易对。 
每根K线的开盘时间可视为唯一ID


**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
interval | ENUM | YES | 详见枚举定义：K线间隔
startTime | LONG | NO |
endTime | LONG | NO |
limit | INT | NO | 默认 500; 最大 1500.
* 如果未发送 startTime 和 endTime ，默认返回最近的交易。



## 24hr 价格变动情况

> **响应**

```javascript 
{
  "symbol": "BTCUSDT",
  "priceChange": "-94.99999800",    //24小时价格变动
  "priceChangePercent": "-95.960",  //24小时价格变动百分比
  "weightedAvgPrice": "0.29628482", //加权平均价
  "prevClosePrice": "3.89000000",   //上一次结束价格
  "lastPrice": "4.00000200",        //最近一次成交价
  "lastQty": "200.00000000",        //最近一次成交额
  "bidPrice": "866.66000000",       //最高的买入价格
  "bidQty": "72.05100000",          //最高的买入价格的数量
  "askPrice": "866.73000000",       //最低的卖出价
  "askQty": "1.21700000",           //最低的卖出价格的数量
  "openPrice": "99.00000000",       //24小时内第一次成交的价格
  "highPrice": "100.00000000",      //24小时最高价
  "lowPrice": "0.10000000",         //24小时最低价
  "volume": "8913.30000000",        //24小时成交量
  "quoteVolume": "15.30000000",     //24小时成交额
  "openTime": 1499783499040,        //24小时内，第一笔交易的发生时间
  "closeTime": 1499869899040,       //24小时内，最后一笔交易的发生时间
  "firstId": 28385,   // 首笔成交id
  "lastId": 28460,    // 末笔成交id
  "count": 76,         // 成交笔数
  "baseAsset": "BTC",   //基础代币
  "quoteAsset": "USDT"  //报价代币
}
```

``
GET /api/v1/ticker/24hr
``

24 小时滚动窗口价格变动数据。 请注意，不携带symbol参数会返回全部交易对数据，此时返回的数据为示例相应的数组，不仅数据庞大，而且权重极高

**权重:**
1 单一交易对; 
**40** 交易对参数缺失;

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
* 请注意，不携带symbol参数会返回全部交易对数据

## 最新价格

> **响应**

```javascript    
{
   "symbol": "ADAUSDT",
   "price": "1.30000000",
   "time": 1649666690902
}  
```

> OR

```javascript
[     
  {
     "symbol": "ADAUSDT",
     "price": "1.30000000",
     "time": 1649666690902
  }
]
```

``
GET /api/v1/ticker/price
``

获取交易对最新价格

**权重:**
1 单一交易对; 
**2** 交易对参数缺失;

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
* 不发送交易对参数，则会返回所有交易对信息


## 当前最优挂单
> **响应**
```javascript
{
  "symbol": "LTCBTC",
  "bidPrice": "4.00000000",
  "bidQty": "431.00000000",
  "askPrice": "4.00000200",
  "askQty": "9.00000000"
  "time": 1589437530011   // 交易时间
}
```
> OR
```javascript
[
  {
    "symbol": "LTCBTC",
    "bidPrice": "4.00000000",
    "bidQty": "431.00000000",
    "askPrice": "4.00000200",
    "askQty": "9.00000000",
    "time": 1589437530011   // 交易时间
  }
]
```

``
GET /api/v1/ticker/bookTicker
``

返回当前最优的挂单(最高买单，最低卖单)

**权重:**
1 单一交易对; 
**2** 交易对参数缺失;

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
* 不发送交易对参数，则会返回所有交易对信息

## 获取Symbol手续费

> **响应**

```javascript
{
   "symbol": "APXUSDT",
   "makerCommissionRate": "0.000200",    
   "takerCommissionRate": "0.000700"
}
```
``
GET /api/v1/commissionRate
``

获取Symbol手续费。

**权重:**
20

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
recvWindow | LONG | NO |赋值不能大于 ```60000```
timestamp | LONG | YES |





# 现货账户和交易接口


## 下单  (TRADE)

> **Response ACK:**

```javascript
{
  "symbol": "BTCUSDT", // 交易对
  "orderId": 28, // 系统的订单ID
  "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP", // 客户自己设置的ID
  "updateTime": 1507725176595, // 交易的时间戳
  "price": "0.00000000", // 订单价格
  "avgPrice": "0.0000000000000000", //平均价格
  "origQty": "10.00000000", // 用户设置的原始订单数量
  "cumQty": "0",            //累计数量
  "executedQty": "10.00000000", // 交易的订单数量
  "cumQuote": "10.00000000", // 累计交易的金额
  "status": "FILLED", // 订单状态
  "timeInForce": "GTC", // 订单的时效方式
  "stopPrice": "0",     //触发价格
  "origType": "LIMIT",  //触发前订单类型
  "type": "LIMIT", // 订单类型， 比如市价单，现价单等
  "side": "SELL", // 订单方向，买还是卖
}
```

``
POST /api/v1/order  (HMAC SHA256)
``

发送下单。

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
side | ENUM | YES | 详见枚举定义：订单方向
type | ENUM | YES | 详见枚举定义：订单类型 
timeInForce | ENUM | NO | 详见枚举定义：有效方式 
quantity | DECIMAL | NO |
quoteOrderQty|DECIMAL|NO|
price | DECIMAL | NO |
newClientOrderId | STRING | NO | 客户自定义的唯一订单ID。 如果未发送，则自动生成
stopPrice | DECIMAL | NO | 仅 `STOP`, `STOP_MARKET` , `TAKE_PROFIT`,`TAKE_PROFIT_MARKET` 需要此参数。
recvWindow | LONG | NO |赋值不能大于 ```60000```
timestamp | LONG | YES |

基于订单 `type`不同，强制要求某些参数:

类型 | 强制要求的参数
------------ | ------------
`LIMIT` | `timeInForce`, `quantity`, `price`
`MARKET` | `quantity` 或 `quoteOrderQty` 
`STOP`和`TAKE_PROFIT` | `quantity`,  `price`, `stopPrice`
`STOP_MARKET`和`TAKE_PROFIT_MARKET` | `quantity`, `stopPrice`

其他信息:

* 下`MARKET` `SELL`市价单，用户通过`QUANTITY`控制想用市价单卖出的基础资产数量。
  * 比如在`BTCUSDT`交易对上下一个`MARKET` `SELL`市价单, 通过`QUANTITY`让用户指明想卖出多少BTC。
* 下`MARKET` `BUY`的市价单，用户使用 `quoteOrderQty` 控制想用市价单买入的报价资产数量，`QUANTITY`将由系统根据市场流动性计算出来。
  * 比如在`BTCUSDT`交易对上下一个`MARKET` `BUY`市价单, 通过`quoteOrderQty`让用户选择想使用多少USDT买入BTC。
* 使用 `quoteOrderQty` 的市价单`MARKET`不会突破`LOT_SIZE`的限制规则; 报单会按给定的`quoteOrderQty`尽可能接近地被执行。
* 除非之前的订单已经成交, 不然设置了相同的`newClientOrderId`订单会被拒绝。



## 撤销订单 (TRADE)

> **响应**

```javascript
{
  "symbol": "BTCUSDT", // 交易对
  "orderId": 28, // 系统的订单ID
  "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP", // 客户自己设置的ID
  "updateTime": 1507725176595, // 交易的时间戳
  "price": "0.00000000", // 订单价格
  "avgPrice": "0.0000000000000000", //平均价格
  "origQty": "10.00000000", // 用户设置的原始订单数量
  "cumQty": "0",            //累计数量
  "executedQty": "10.00000000", // 交易的订单数量
  "cumQuote": "10.00000000", // 累计交易的金额
  "status": "CANCELED", // 订单状态
  "timeInForce": "GTC", // 订单的时效方式
  "stopPrice": "0",     //触发价格
  "origType": "LIMIT",  //触发前订单类型
  "type": "LIMIT", // 订单类型， 比如市价单，现价单等
  "side": "SELL", // 订单方向，买还是卖
}
```

``
DELETE /api/v1/order  (HMAC SHA256)
``

取消有效订单。

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
origClientOrderId | STRING | NO |
recvWindow | LONG | NO | 
timestamp | LONG | YES |

`orderId` 或 `origClientOrderId` 必须至少发送一个

## 查询订单 (USER_DATA)

> **响应**
```javascript
{
    "orderId": 38,   // 系统订单号
    "symbol": "ADA25SLP25",  // 交易对
    "status": "FILLED",  // 订单状态
    "clientOrderId": "afMd4GBQyHkHpGWdiy34Li",  // 用户自定义的订单号
    "price": "20",  // 委托价格
    "avgPrice": "12.0000000000000000",  // 平均成交价
    "origQty": "10",  // 原始委托数量
    "executedQty": "10",  // 成交量
    "cumQuote": "120",  // 成交金额
    "timeInForce": "GTC",  // 有效方法
    "type": "LIMIT",  // 订单类型
    "side": "BUY",  // 买卖方向
    "stopPrice": "0",  // 触发价
    "origType": "LIMIT",  // 触发前订单类型
    "time": 1649913186270,  // 订单时间
    "updateTime": 1649913186297  // 更新时间
}
```

``
GET /api/v1/order (HMAC SHA256)
``

查询订单状态。

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单的最终状态为 `CANCELED` 或者 `EXPIRED`, **并且** 
	* 订单没有任何的成交记录, **并且**
	* 订单生成时间 + 7天 < 当前时间

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
origClientOrderId | STRING | NO |
recvWindow | LONG | NO |  
timestamp | LONG | YES |

注意:

* 至少需要发送 `orderId` 与 `origClientOrderId`中的一个


## 查询当前挂单 (USER_DATA)

> **响应**
```javascript
{
    "orderId": 38,   // 系统订单号
    "symbol": "ADA25SLP25",  // 交易对
    "status": "NEW",  // 订单状态
    "clientOrderId": "afMd4GBQyHkHpGWdiy34Li",  // 用户自定义的订单号
    "price": "20",  // 委托价格
    "avgPrice": "12.0000000000000000",  // 平均成交价
    "origQty": "10",  // 原始委托数量
    "executedQty": "10",  // 成交量
    "cumQuote": "120",  // 成交金额
    "timeInForce": "GTC",  // 有效方法
    "type": "LIMIT",  // 订单类型
    "side": "BUY",  // 买卖方向
    "stopPrice": "0",  // 触发价
    "origType": "LIMIT",  // 触发前订单类型
    "time": 1649913186270,  // 订单时间
    "updateTime": 1649913186297  // 更新时间
}
```

``
GET /api/v1/openOrder (HMAC SHA256)
``

查询订单状态。

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
origClientOrderId | STRING | NO |
recvWindow | LONG | NO |  
timestamp | LONG | YES |

注意:

* 至少需要发送 `orderId` 与 `origClientOrderId`中的一个


## 当前所有挂单 (USER_DATA)

> **响应**

```javascript
[
    {
        "orderId": 349661, // 系统订单号
        "symbol": "BNBUSDT", // 交易对
        "status": "NEW", // 订单状态
        "clientOrderId": "LzypgiMwkf3TQ8wwvLo8RA", // 用户自定义的订单号
        "price": "1.10000000", // 委托价格
        "avgPrice": "0.0000000000000000", // 平均成交价
        "origQty": "5",  // 原始委托数量
        "executedQty": "0", // 成交量
        "cumQuote": "0", // 成交金额
        "timeInForce": "GTC", // 有效方法
        "type": "LIMIT", // 订单类型
        "side": "BUY",   // 买卖方向
        "stopPrice": "0", // 触发价
        "origType": "LIMIT", // 触发前订单类型
        "time": 1756252940207, // 订单时间
        "updateTime": 1756252940207, // 更新时间
    }
]
```

``
GET /api/v1/openOrders  (HMAC SHA256)
``

获取交易对的所有当前挂单， 请小心使用不带交易对参数的调用。

**权重:**
- 带symbol ***1***
- 不带 ***40***  

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
recvWindow | LONG | NO |
timestamp | LONG | YES |

* 不带symbol参数，会返回所有交易对的挂单



## 取消当前所有挂单 (USER_DATA)

> **响应**

```javascript
{
    "code": 200,
    "msg": "The operation of cancel all open order is done."
}
```

``
DEL /api/v1/allOpenOrders  (HMAC SHA256)
``

**权重:**
- ***1***

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderIdList | STRING | NO | id数组字符串
origClientOrderIdList | STRING | NO | clientOrderId数组字符串
recvWindow | LONG | NO |
timestamp | LONG | YES |


## 查询所有订单 (USER_DATA)
> **响应**
```javascript
[
    {
        "orderId": 349661, // 系统订单号
        "symbol": "BNBUSDT", // 交易对
        "status": "NEW", // 订单状态
        "clientOrderId": "LzypgiMwkf3TQ8wwvLo8RA", // 用户自定义的订单号
        "price": "1.10000000", // 委托价格
        "avgPrice": "0.0000000000000000", // 平均成交价
        "origQty": "5",  // 原始委托数量
        "executedQty": "0", // 成交量
        "cumQuote": "0", // 成交金额
        "timeInForce": "GTC", // 有效方法
        "type": "LIMIT", // 订单类型
        "side": "BUY",   // 买卖方向
        "stopPrice": "0", // 触发价
        "origType": "LIMIT", // 触发前订单类型
        "time": 1756252940207, // 订单时间
        "updateTime": 1756252940207, // 更新时间
    }
]
```

``
GET /api/v1/allOrders (HMAC SHA256)
``

获取所有帐户订单； 有效，已取消或已完成。

* 请注意，如果订单满足如下条件，不会被查询到：
	* 订单生成时间 + 7天 < 当前时间

**权重:** 
5

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | YES |
orderId | LONG | NO |
startTime | LONG | NO |
endTime | LONG | NO |
limit | INT | NO | 默认 500; 最大 1000.
recvWindow | LONG | NO | 
timestamp | LONG | YES |  

* 查询时间范围最大不得超过7天
* 默认查询最近7天内的数据 


## 期货现货互转 (TRADE)

> **响应:**

```javascript
{
    "tranId": 21841, //交易id
    "status": "SUCCESS" //状态
}
```

``
POST /api/v1/asset/wallet/transfer  (HMAC SHA256)
``

**权重:**
5

**参数:**


名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	数量
asset |	STRING | 	YES |	资产
clientTranId |	STRING | 	YES |	交易id 
kindType |	STRING | 	YES |	交易类型
recvWindow | LONG | NO | 
timestamp	| LONG | YES	|	时间戳

注意:
* kindType 取值为FUTURE_SPOT(期货转现货),SPOT_FUTURE(现货转期货)

## 转账给其他地址账户 (TRADE)

> **响应:**

```javascript
{
    "tranId": 21841, //交易id
    "status": "SUCCESS" //状态
}
```

``
POST /api/v1/asset/sendToAddress  (HMAC SHA256)
``

**权重:**
1

**参数:**


名称              |  类型   | 是否必需   | 描述
---------------- | ------- | -------- | ----
amount |	DECIMAL | 	YES |	数量
asset |	STRING | 	YES |	资产
toAddress |	STRING | 	YES |	目标地址
clientTranId |	STRING | 	NO |	交易id 
recvWindow | LONG | NO | 
timestamp	| LONG | YES	|	时间戳

注意:
* toAddress必须存在, 且不能为发送方账户
* toAddress为evm地址
* clientTranId如果传入则长度最少为20



## 现货提现手续费 (NONe)
> **响应**
```javascript
{
  "tokenPrice": 1.00019000,
   "gasCost": 0.5000,
  "gasUsdValue": 0.5
}
```

``
GET /api/v1/aster/withdraw/estimateFee 
``

**权重:** 
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
chainId | STRING | YES | 
asset | STRING | YES |

注意:
* chainId: 1(ETH),56(BSC),42161(Arbi)
* gasCost: 提现所需的最少数量的手续费

## 现货提现 (USER_DATA)
> **响应**
```javascript
{
  "withdrawId": "1014729574755487744",
  "hash":"0xa6d1e617a3f69211df276fdd8097ac8f12b6ad9c7a49ba75bbb24f002df0ebb"
}
```

``
POST /api/v1/aster/user-withdraw (HMAC SHA256)
``

**权重:** 
5

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
chainId | STRING | YES | 
asset | STRING | YES |
amount | STRING | YES |
fee | STRING | YES |
receiver | STRING | YES | 
nonce | STRING | YES |  当前时间的微秒值 
userSignature | STRING | YES | 
recvWindow | LONG | NO | 
timestamp | LONG | YES | 

注意:
* chainId: 1(ETH),56(BSC),42161(Arbi)
* receiver : 当前账户的地址
* 如果期货余额不足，会从spot账户划转到期货账户，进行提现
* userSignature demo

```shell
const domain = {
    name: 'Aster',
    version: '1',
    chainId: 56,
    verifyingContract: ethers.ZeroAddress,
  }

const currentTime = Date.now() * 1000
 
const types = {
    Action: [
        {name: "type", type: "string"},
        {name: "destination", type: "address"},
        {name: "destination Chain", type: "string"},
        {name: "token", type: "string"},
        {name: "amount", type: "string"},
        {name: "fee", type: "string"},
        {name: "nonce", type: "uint256"},
        {name: "aster chain", type: "string"},
    ],
  }
  const value = {
    'type': 'Withdraw',
    'destination': '0xD9cA6952F1b1349d27f91E4fa6FB8ef67b89F02d',
    'destination Chain': 'BSC',
    'token': 'USDT',
    'amount': '10.123400',
    'fee': '1.234567891',
    'nonce': currentTime,
    'aster chain': 'Mainnet',
  }


const signature = await signer.signTypedData(domain, types, value)
```

## 获取创建apikey nonce (NONE)
> **响应**
```javascript

111111

```

``
POST /api/v1/getNonce 
``

**权重:** 
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
address | STRING | YES |
userOperationType | STRING | YES | CREATE_API_KEY
network | STRING | NO | 

注意:
* userOperationType 仅可取值: CREATE_API_KEY
* network: sol网络必须传入SOL,其他忽略
* 限流单IP一分钟60次


## 创建apikey (NONE)
> **响应**
```javascript
{
    "apiKey": "bb3b24d0a3dec88cb06be58a257e4575cb0b1bb256ad6fd90ae8fd0ee1d102ae",
    "apiSecret": "9fe8f5642ae1961674ea0cb7f957fa99dc8e0421b607c985a963ad2ced90ae1c"
}
```

``
POST /api/v1/createApiKey
``

**权重:** 
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
address | STRING | YES |
userOperationType | STRING | YES | CREATE_API_KEY
network | STRING | NO | 
userSignature | STRING | YES | 
apikeyIP | STRING | NO | 
desc | STRING | YES | 
recvWindow | LONG | NO | 
timestamp | LONG | YES | 

注意:
* userOperationType 仅可取值: CREATE_API_KEY
* network: sol网络必须传入SOL,其他忽略
* desc: 同一账户不能重复，长度不能超过20个字符
* apikeyIP ip数组以,分隔
* 限流单IP一分钟60次
* userSignature evm demo

```shell

const nonce = 111111
const message = 'You are signing into Astherus ${nonce}' ;

const signature = await signer.signMessage(message);
```


## 账户信息 (USER_DATA)
> **响应**
```javascript
{     
   "feeTier": 0,
   "canTrade": true,
   "canDeposit": true,
   "canWithdraw": true,
   "canBurnAsset": true,
   "updateTime": 0,
   "balances": [
    {
      "asset": "BTC",
      "free": "4723846.89208129",
      "locked": "0.00000000"
    },
    {
      "asset": "LTC",
      "free": "4763368.68006011",
      "locked": "0.00000000"
    }
  ]
}
```

``
GET /api/v1/account (HMAC SHA256)
``

获取当前账户信息。

**权重:**
5

**参数:**

名称 | 类型 | 是否必需| 描述
------------ | ------------ | ------------ | ------------
recvWindow | LONG | NO | 
timestamp | LONG | YES |


## 账户成交历史 (USER_DATA)
> **响应**
```javascript 
[ 
  {
    "symbol": "BNBUSDT",
    "id": 1002,
    "orderId": 266358,
    "side": "BUY",
    "price": "1",
    "qty": "2",
    "quoteQty": "2",
    "commission": "0.00105000",
    "commissionAsset": "BNB",
    "time": 1755656788798,
    "counterpartyId": 19,
    "createUpdateId": null,
    "maker": false,
    "buyer": true
  }
] 
```

``
GET /api/v1/userTrades  (HMAC SHA256)
``

获取账户指定交易对的成交历史

**权重:**
5

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
symbol | STRING | NO |
orderId|LONG|NO| 必须要和参数`symbol`一起使用.
startTime | LONG | NO |
endTime | LONG | NO |
fromId | LONG | NO | 起始Trade id。 默认获取最新交易。
limit | INT | NO | 默认 500; 最大 1000.
recvWindow | LONG | NO | 
timestamp | LONG | YES |

* 如果`startTime` 和 `endTime` 均未发送, 只会返回最近7天的数据。
* startTime 和 endTime 的最大间隔为7天
* 不能同时传`fromId`与`startTime` 或 `endTime`
       




---
# Websocket 行情推送

* 本篇所列出的所有wss接口的baseurl为: **wss://sstream.asterdex.com**
* Streams有单一原始 stream 或组合 stream
* 单一原始 streams 格式为 **/ws/\<streamName\>**
* 组合streams的URL格式为 **/stream?streams=\<streamName1\>/\<streamName2\>/\<streamName3\>**
* 订阅组合streams时，事件payload会以这样的格式封装: **{"stream":"\<streamName\>","data":\<rawPayload\>}**
* stream名称中所有交易对均为 **小写**
* 每个到 **sstream.asterdex.com** 的链接有效期不超过24小时，请妥善处理断线重连。
* 每3分钟，服务端会发送ping帧，客户端应当在10分钟内回复pong帧，否则服务端会主动断开链接。允许客户端发送不成对的pong帧(即客户端可以以高于10分钟每次的频率发送pong帧保持链接)。

## 实时订阅/取消数据流

* 以下数据可以通过websocket发送以实现订阅或取消订阅数据流。示例如下。
* 响应内容中的`id`是无符号整数，作为往来信息的唯一标识。
* 如果相应内容中的 `result` 为 `null`，表示请求发送成功。
  

### 订阅一个信息流
> **响应**
  ```javascript
  {
    "result": null,
    "id": 1
  }
  ```
* **请求**
  	{    
    	"method": "SUBSCRIBE",    
    	"params":     
    	[   
      	"btcusdt@aggTrade",    
      	"btcusdt@depth"     
    	],    
    	"id": 1   
  	}


### 取消订阅一个信息流

> **响应**
  
  ```javascript
  {
    "result": null,
    "id": 312
  }
  ```

* **请求**
  {   
    "method": "UNSUBSCRIBE",    
    "params":     
    [    
      "btcusdt@depth"   
    ],    
    "id": 312   
  }


### 已订阅信息流

> **响应**
  
  ```javascript
  {
    "result": [
      "btcusdt@aggTrade"
    ],
    "id": 3
  }
  ```

* **请求**

  {   
    "method": "LIST_SUBSCRIPTIONS",    
    "id": 3   
  }     
 

### 设定属性
当前，唯一可以设置的属性是设置是否启用`combined`("组合")信息流。   
当使用`/ws/`("原始信息流")进行连接时，combined属性设置为`false`，而使用 `/stream/`进行连接时则将属性设置为`true`。

> **响应**
  
  ```javascript
{
  "result": null,
  "id": 5
}
  ```

* **请求**
  {    
    "method": "SET_PROPERTY",    
    "params":     
    [   
      "combined",    
      true   
    ],    
    "id": 5   
  }


### 检索属性

> **响应**
  ```javascript
  {
    "result": true, // Indicates that combined is set to true.
    "id": 2
  }
  ```
  
* **请求**
  
  {   
    "method": "GET_PROPERTY",    
    "params":     
    [   
      "combined"   
    ],    
    "id": 2   
  }   
 

###错误信息

错误信息 | 描述
---|---
{"code": 0, "msg": "Unknown property"} |  `SET_PROPERTY` 或 `GET_PROPERTY`中应用的参数无效
{"code": 1, "msg": "Invalid value type: expected Boolean", "id": '%s'} | 仅接受`true`或`false`
{"code": 2, "msg": "Invalid request: property name must be a string"}| 提供的属性名无效
{"code": 2, "msg": "Invalid request: request ID must be an unsigned integer"}| 参数`id`未提供或`id`值是无效类型
{"code": 2, "msg": "Invalid request: unknown variant %s, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28"} | 错字提醒，或提供的值不是预期类型
{"code": 2, "msg": "Invalid request: too many parameters"}| 数据中提供了不必要参数
{"code": 2, "msg": "Invalid request: property name must be a string"} | 未提供属性名
{"code": 2, "msg": "Invalid request: missing field `method` at line 1 column 73"} | 数据未提供`method`
{"code":3,"msg":"Invalid JSON: expected value at line %s column %s"} | JSON 语法有误.


## 归集交易流


> **Payload:**
```javascript
{
  "e": "aggTrade",  // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBBTC",    // 交易对
  "a": 12345,       // 归集交易ID
  "p": "0.001",     // 成交价格
  "q": "100",       // 成交数量
  "f": 100,         // 被归集的首个交易ID
  "l": 105,         // 被归集的末次交易ID
  "T": 123456785,   // 成交时间
  "m": true,        // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
}
```

归集交易 stream 推送交易信息，是对单一订单的集合。

**Stream 名称:** `<symbol>@aggTrade`   

**Update Speed:** 实时


## 逐笔交易


> **Payload:**
```javascript
{
  "e": "trade",     // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBBTC",    // 交易对
  "t": 12345,       // 交易ID
  "p": "0.001",     // 成交价格
  "q": "100",       // 成交数量
  "T": 123456785,   // 成交时间
  "m": true,        // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
}
```

**Stream Name:** `<symbol>@trade`

逐笔交易推送每一笔成交的信息。**成交**，或者说交易的定义是仅有一个吃单者与一个挂单者相互交易


## K线 Streams
> **Payload:**
```javascript
{
  "e": "kline",     // 事件类型
  "E": 123456789,   // 事件时间
  "s": "BNBBTC",    // 交易对
  "k": {
    "t": 123400000, // 这根K线的起始时间
    "T": 123460000, // 这根K线的结束时间
    "s": "BNBBTC",  // 交易对
    "i": "1m",      // K线间隔
    "f": 100,       // 这根K线期间第一笔成交ID
    "L": 200,       // 这根K线期间末一笔成交ID
    "o": "0.0010",  // 这根K线期间第一笔成交价
    "c": "0.0020",  // 这根K线期间末一笔成交价
    "h": "0.0025",  // 这根K线期间最高成交价
    "l": "0.0015",  // 这根K线期间最低成交价
    "v": "1000",    // 这根K线期间成交量
    "n": 100,       // 这根K线期间成交笔数
    "x": false,     // 这根K线是否完结(是否已经开始下一根K线)
    "q": "1.0000",  // 这根K线期间成交额
    "V": "500",     // 主动买入的成交量
    "Q": "0.500",   // 主动买入的成交额
    "B": "123456"   // 忽略此参数
  }
}
```

K线stream逐秒推送所请求的K线种类(最新一根K线)的更新。

**Stream Name:** `<symbol>@kline_<interval>` 
   
**Update Speed:** 2000ms

**K线图间隔参数:**

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w
* 1M


## 按 Symbol 的精简Ticker

> **Payload:**
```javascript
  {
    "e": "24hrMiniTicker",  // 事件类型
    "E": 123456789,         // 事件时间
    "s": "BNBBTC",          // 交易对
    "c": "0.0025",          // 最新成交价格
    "o": "0.0010",          // 24小时前开始第一笔成交价格
    "h": "0.0025",          // 24小时内最高成交价
    "l": "0.0010",          // 24小时内最低成交价
    "v": "10000",           // 成交量
    "q": "18"               // 成交额
  }
```

按Symbol刷新的最近24小时精简ticker信息

**Stream 名称:** `<symbol>@miniTicker`

**Update Speed:** 1000ms


## 全市场所有Symbol的精简Ticker

> **Payload:**
```javascript
[
  {
    // 数组每一个元素对应一个交易对，内容与 \<symbol\>@miniTicker相同
  }
]
```

同上，只是推送所有交易对.需要注意的是，只有更新的ticker才会被推送.

**Stream名称:** !miniTicker@arr

**Update Speed:** 1000ms

## 按Symbol的完整Ticker

> **Payload:**
```javascript
{
  "e": "24hrTicker",  // 事件类型
  "E": 123456789,     // 事件时间
  "s": "BNBBTC",      // 交易对
  "p": "0.0015",      // 24小时价格变化
  "P": "250.00",      // 24小时价格变化(百分比)
  "w": "0.0018",      // 平均价格
  "c": "0.0025",      // 最新成交价格
  "Q": "10",          // 最新成交交易的成交量
  "o": "0.0010",      // 整整24小时前，向后数的第一次成交价格
  "h": "0.0025",      // 24小时内最高成交价
  "l": "0.0010",      // 24小时内最低成交价
  "v": "10000",       // 24小时内成交量
  "q": "18",          // 24小时内成交额
  "O": 0,             // 统计开始时间
  "C": 86400000,      // 统计结束时间
  "F": 0,             // 24小时内第一笔成交交易ID
  "L": 18150,         // 24小时内最后一笔成交交易ID
  "n": 18151          // 24小时内成交数
}
```

每秒推送单个交易对的过去24小时滚动窗口标签统计信息。

**Stream 名称:** `<symbol>@ticker`

**Update Speed:** 1000ms

## 全市场所有交易对的完整Ticker

> **Payload:**
```javascript
[
  {
    // Same as <symbol>@ticker payload
  }
]
```

推送全市场所有交易对刷新的24小时完整ticker信息。需要注意的是，没有更新的ticker不会被推送。

**Stream Name:** `!ticker@arr`

**Update Speed:** 1000ms


## 按Symbol的最优挂单信息

> **Payload:**
```javascript
{
  "u":400900217,     // order book updateId
  "s":"BNBUSDT",     // 交易对
  "b":"25.35190000", // 买单最优挂单价格
  "B":"31.21000000", // 买单最优挂单数量
  "a":"25.36520000", // 卖单最优挂单价格
  "A":"40.66000000"  // 卖单最优挂单数量
}
```

实时推送指定交易对最优挂单信息

**Stream Name:** `<symbol>@bookTicker`

**Update Speed:** 实时

## 全市场最优挂单信息
> **Payload:**
```javascript
{
  // 同 <symbol>@bookTicker payload
}
```

实时推送所有交易对最优挂单信息

**Stream Name:** `!bookTicker`

**Update Speed:** 实时


## 有限档深度信息

> **Payload:**
```javascript 
{ 
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 100,           // First update ID in event
  "u": 120,           // Final update ID in event
  "pu": 99,          // Final update Id in last stream(ie `u` in last stream) 
  "bids": [             // Bids to be updated
    [
      "0.0024",         // Price level to be updated
      "10"              // Quantity
    ]
  ],
  "asks": [             // Asks to be updated
    [
      "0.0026",         // Price level to be updated
      "100"             // Quantity
    ]
  ]
} 
```

每秒或每100毫秒推送有限档深度信息。levels表示几档买卖单信息, 可选 5/10/20档

**Stream Names:** `<symbol>@depth<levels>` 或 `<symbol>@depth<levels>@100ms`.  

**Update Speed:** 1000ms 或 100ms


## 增量深度信息
> **Payload:**
```javascript  
{
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "T": 123456788,     // Transaction time 
  "s": "BTCUSDT",     // Symbol
  "U": 100,           // First update ID in event
  "u": 120,           // Final update ID in event
  "pu": 99,          // Final update Id in last stream(ie `u` in last stream)
  "b": [              // Bids to be updated
    [
      "5.4",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "5.6",       // Price level to be updated
      "100"          // Quantity
    ]
  ]
}   
```

每秒或每100毫秒推送orderbook的变化部分(如果有)

**Stream Name:** `<symbol>@depth` 或 `<symbol>@depth@100ms`

**Update Speed:** 1000ms 或 100ms

## 如何正确在本地维护一个orderbook副本
1. 订阅 **wss://sstream.asterdex.com/ws/bnbbtc@depth**
2. 开始缓存收到的更新。同一个价位，后收到的更新覆盖前面的。
3. 访问Rest接口 **https://sapi.asterdex.com/api/v1/depth?symbol=BNBBTC&limit=1000** 获得一个1000档的深度快照
4. 将目前缓存到的信息中`u` <= 步骤3中获取到的快照中的`lastUpdateId`的部分丢弃(丢弃更早的信息，已经过期)。
5. 将深度快照中的内容更新到本地orderbook副本中，并从websocket接收到的第一个`U` <= `lastUpdateId`+1 **且** `u` >= `lastUpdateId`+1 的event开始继续更新本地副本。
6. 每一个新event的`U`应该恰好等于上一个event的`u`+1，否则可能出现了丢包，请从step3重新进行初始化。
7. 每一个event中的挂单量代表这个价格目前的挂单量**绝对值**，而不是相对变化。
8. 如果某个价格对应的挂单量为0，表示该价位的挂单已经撤单或者被吃，应该移除这个价位。






# Websocket账户信息推送

* 本篇所列出API接口的base url : **https://sapi.asterdex.com**
* 用于订阅账户数据的 `listenKey` 从创建时刻起有效期为60分钟
* 可以通过 `PUT` 一个 `listenKey` 延长60分钟有效期
* 可以通过`DELETE`一个 `listenKey` 立即关闭当前数据流，并使该`listenKey` 无效
* 在具有有效`listenKey`的帐户上执行`POST`将返回当前有效的`listenKey`并将其有效期延长60分钟
* websocket接口的baseurl: **wss://sstream.asterdex.com**
* U订阅账户数据流的stream名称为 **/ws/\<listenKey\>**
* 每个链接有效期不超过24小时，请妥善处理断线重连。


## Listen Key(现货账户)

### 生成 Listen Key (USER_STREAM)

> **响应**
```javascript
{
  "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
}
```

``
POST /api/v1/listenKey
``

开始一个新的数据流。除非发送 keepalive，否则数据流于60分钟后关闭。如果该帐户具有有效的`listenKey`，则将返回该`listenKey`并将其有效期延长60分钟。

**权重:**
1

**参数:**
NONE

### 延长 Listen Key 有效期 (USER_STREAM)

> **响应**
```javascript
{}
```

``
PUT /api/v1/listenKey 
``

有效期延长至本次调用后60分钟,建议每30分钟发送一个 ping 。

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
listenKey | STRING | YES


### 关闭 Listen Key (USER_STREAM)

> **响应**
```javascript
{}
```

``
DELETE /api/v1/listenKey
``

关闭用户数据流。

**权重:**
1

**参数:**

名称 | 类型 | 是否必需 | 描述
------------ | ------------ | ------------ | ------------
listenKey | STRING | YES


## Payload: 账户更新

每当帐户余额发生更改时，都会发送一个事件`outboundAccountPosition`，其中包含可能由生成余额变动的事件而变动的资产。

> **Payload**
```javascript
{
  "B":[// 余额
    {
      "a":"SLP25",   // 资产名称
      "f":"10282.42029415",   // 可用余额
      "l":"653.00000001"   // 冻结余额
    },
    {
      "a":"ADA25",
      "f":"9916.96229880",
      "l":"34.00510000"
    }
  ],
  "e":"outboundAccountPosition",   // 事件类型
  "T":1649926447190,     // 账户末次更新时间戳 
  "E":1649926447205   // 事件时间
  "m":"WITHDRAW" // 事件推出原因
}
```


## Payload: 订单更新

订单通过`executionReport`事件进行更新。 

> **Payload**

```javascript
{
  "s":"ADA25SLP25",   // 交易对
  "c":"Xzh0gnxT41PStbwqOtXnjD",  // 客户端自定订单ID
  "S":"SELL",   // 订单方向
  "o":"LIMIT",   // 订单类型
  "f":"GTC",   // 有效方式
  "q":"10.001000",   // 订单原始数量
  "p":"19.1000000000",   // 订单原始价格
  "ap":"19.0999999955550656",  //平均价格
  "P":"0",  // 条件订单触发价格
  "x":"TRADE",   // 本次事件的具体执行类型
  "X":"PARTIALLY_FILLED",   // 订单的当前状态
  "i":27,   // 订单ID
  "l":"1",      // 订单末次成交量 
  "z":"8.999000",   // 订单累计已成交量
  "L":"19.1000000000",   // 订单末次成交价格
  "n":"0.00382000",    // 手续费数量    
  "N":"SLP25",   // 手续费资产类型
  "T":1649926447190,   // 成交时间
  "t":18,   // 成交ID
  "m":true,   // 该成交是作为挂单成交吗？
  "ot":"LIMIT",  //初始订单类型
  "O":0,   // 订单时间
  "Z":"171.88089996",   // 累计报价资产交易数量
  "Y":"19.1000000000000000",   // 最近报价交易数量
  "Q":"0",   // 报价数量
  "e":"executionReport",   // 事件类型
  "E":1649926447209  // 事件时间
}  
```


**执行类型:**
* NEW 新订单
* CANCELED 订单被取消
* REJECTED 新订单被拒绝
* TRADE 订单有新成交
* EXPIRED 订单失效(根据订单的Time In Force参数)






#错误代码

> error JSON payload:
 
```javascript
{
  "code":-1121,
  "msg":"Invalid symbol."
}
```

错误由两部分组成：错误代码和消息。 代码是通用的，但是消息可能会有所不同。


## 10xx - 常规服务器或网络问题
### -1000 UNKNOWN
 * An unknown error occured while processing the request.
 * 处理请求时发生未知错误。

### -1001 DISCONNECTED
 * Internal error; unable to process your request. Please try again.
 * 内部错误; 无法处理您的请求。 请再试一次.

### -1002 UNAUTHORIZED
 * You are not authorized to execute this request.
 * 您无权执行此请求。

### -1003 TOO_MANY_REQUESTS
 * Too many requests queued.
 * 排队的请求过多。
 * Too many requests; please use the websocket for live updates.
 * 请求权重过多； 请使用websocket获取最新更新。
 * Too many requests; current limit is %s requests per minute. Please use the websocket for live updates to avoid polling the API.
 * 请求权重过多； 当前限制为每分钟％s请求权重。 请使用websocket进行实时更新，以避免轮询API。
 * Way too many requests; IP banned until %s. Please use the websocket for live updates to avoid bans.
 * 请求权重过多； IP被禁止，直到％s。 请使用websocket进行实时更新，以免被禁。
 
### -1004 DUPLICATE_IP
 * This IP is already on the white list
 * IP地址已经在白名单

### -1005 NO_SUCH_IP
 * No such IP has been white listed
 * 白名单上没有此IP地址
 
### -1006 UNEXPECTED_RESP
 * An unexpected response was received from the message bus. Execution status unknown.
 * 从消息总线收到意外的响应。执行状态未知。

### -1007 TIMEOUT
 * Timeout waiting for response from backend server. Send status unknown; execution status unknown.
 * 等待后端服务器响应超时。 发送状态未知； 执行状态未知。

### -1014 UNKNOWN_ORDER_COMPOSITION
 * Unsupported order combination.
 * 不支持当前的下单参数组合

### -1015 TOO_MANY_ORDERS
 * Too many new orders.
 * 新订单太多。
 * Too many new orders; current limit is %s orders per %s.
 * 新订单太多； 当前限制为每％s ％s个订单。

### -1016 SERVICE_SHUTTING_DOWN
 * This service is no longer available.
 * 该服务不可用。

### -1020 UNSUPPORTED_OPERATION
 * This operation is not supported.
 * 不支持此操作。

### -1021 INVALID_TIMESTAMP
 * Timestamp for this request is outside of the recvWindow.
  * 此请求的时间戳在recvWindow之外。
 * Timestamp for this request was 1000ms ahead of the server's time.
 * 此请求的时间戳比服务器时间提前1000毫秒。

### -1022 INVALID_SIGNATURE
 * Signature for this request is not valid.
 * 此请求的签名无效。

### -1023 START_TIME_GREATER_THAN_END_TIME
 * Start time is greater than end time.
 * 参数里面的开始时间在结束时间之后


## 11xx - Request issues
### -1100 ILLEGAL_CHARS
 * Illegal characters found in a parameter.
 * 在参数中发现非法字符。
 * Illegal characters found in parameter '%s'; legal range is '%s'.
 * 在参数`％s`中发现非法字符； 合法范围是`％s`。

### -1101 TOO_MANY_PARAMETERS
 * Too many parameters sent for this endpoint.
 * 为此端点发送的参数太多。
 * Too many parameters; expected '%s' and received '%s'.
 * 参数太多；预期为`％s`并收到了`％s`。
 * Duplicate values for a parameter detected.
 * 检测到的参数值重复。

### -1102 MANDATORY_PARAM_EMPTY_OR_MALFORMED
 * A mandatory parameter was not sent, was empty/null, or malformed.
 * 未发送强制性参数，该参数为空/空或格式错误。
 * Mandatory parameter '%s' was not sent, was empty/null, or malformed.
 * 强制参数`％s`未发送，为空/空或格式错误。
 * Param '%s' or '%s' must be sent, but both were empty/null!
 * 必须发送参数`％s`或`％s`，但两者均为空！

### -1103 UNKNOWN_PARAM
 * An unknown parameter was sent.
 * 发送了未知参数。

### -1104 UNREAD_PARAMETERS
 * Not all sent parameters were read.
 * 并非所有发送的参数都被读取。
 * Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.
 * 并非所有发送的参数都被读取； 读取了`％s`参数，但被发送了`％s`。

### -1105 PARAM_EMPTY
 * A parameter was empty.
 * 参数为空。
 * Parameter '%s' was empty.
 * 参数`％s`为空。

### -1106 PARAM_NOT_REQUIRED
 * A parameter was sent when not required.
 * 发送了不需要的参数。
 * Parameter '%s' sent when not required.
 * 发送了不需要参数`％s`。

### -1111 BAD_PRECISION
 * Precision is over the maximum defined for this asset.
 * 精度超过为此资产定义的最大值。

### -1112 NO_DEPTH
 * No orders on book for symbol.
 * 交易对没有挂单。
 
### -1114 TIF_NOT_REQUIRED
 * TimeInForce parameter sent when not required.
 * 发送的`TimeInForce`参数不需要。

### -1115 INVALID_TIF
 * Invalid timeInForce.
 * 无效的`timeInForce`

### -1116 INVALID_ORDER_TYPE
 * Invalid orderType.
 * 无效订单类型。

### -1117 INVALID_SIDE
 * Invalid side.
 * 无效买卖方向。

### -1118 EMPTY_NEW_CL_ORD_ID
 * New client order ID was empty.
 * 新的客户订单ID为空。

### -1119 EMPTY_ORG_CL_ORD_ID
 * Original client order ID was empty.
 * 客户自定义的订单ID为空。

### -1120 BAD_INTERVAL
 * Invalid interval.
 * 无效时间间隔。

### -1121 BAD_SYMBOL
 * Invalid symbol.
 * 无效的交易对。

### -1125 INVALID_LISTEN_KEY
 * This listenKey does not exist.
 * 此`listenKey`不存在。

### -1127 MORE_THAN_XX_HOURS
 * Lookup interval is too big.
 * 查询间隔太大。
 * More than %s hours between startTime and endTime.
 * 从开始时间到结束时间之间超过`％s`小时。

### -1128 OPTIONAL_PARAMS_BAD_COMBO
 * Combination of optional parameters invalid.
 * 可选参数组合无效。

### -1130 INVALID_PARAMETER
 * Invalid data sent for a parameter.
 * 发送的参数为无效数据。
 * Data sent for parameter '%s' is not valid.
 * 发送参数`％s`的数据无效。

### -1136 INVALID_NEW_ORDER_RESP_TYPE
 * Invalid newOrderRespType.
 * 无效的 newOrderRespType。


## 20xx - Processing Issues
### -2010 NEW_ORDER_REJECTED
 * NEW_ORDER_REJECTED
 * 新订单被拒绝

### -2011 CANCEL_REJECTED
 * CANCEL_REJECTED
 * 取消订单被拒绝

### -2013 NO_SUCH_ORDER
 * Order does not exist.
 * 订单不存在。

### -2014 BAD_API_KEY_FMT
 * API-key format invalid.
 * API-key 格式无效。

### -2015 REJECTED_MBX_KEY
 * Invalid API-key, IP, or permissions for action.
 * 无效的API密钥，IP或操作权限。

### -2016 NO_TRADING_WINDOW
 * No trading window could be found for the symbol. Try ticker/24hrs instead.
 * 找不到该交易对的交易窗口。 尝试改为24小时自动报价。

### -2018 BALANCE_NOT_SUFFICIENT
 * Balance is insufficient.
 * 余额不足

### -2020 UNABLE_TO_FILL
 * Unable to fill.
 * 无法成交

### -2021 ORDER_WOULD_IMMEDIATELY_TRIGGER
 * Order would immediately trigger.
 * 订单可能被立刻触发

### -2022 REDUCE_ONLY_REJECT
 * ReduceOnly Order is rejected.
 * `ReduceOnly`订单被拒绝

### -2024 POSITION_NOT_SUFFICIENT
 * Position is not sufficient.
 * 持仓不足

### -2025 MAX_OPEN_ORDER_EXCEEDED
 * Reach max open order limit.
 * 挂单量达到上限

### -2026 REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED
 * This OrderType is not supported when reduceOnly.
 * 当前订单类型不支持`reduceOnly`

## 40xx - Filters and other Issues
### -4000 INVALID_ORDER_STATUS
 * Invalid order status.
 * 订单状态不正确

### -4001 PRICE_LESS_THAN_ZERO
 * Price less than 0.
 * 价格小于0

### -4002 PRICE_GREATER_THAN_MAX_PRICE
 * Price greater than max price.
 * 价格超过最大值
 
### -4003 QTY_LESS_THAN_ZERO
 * Quantity less than zero.
 * 数量小于0

### -4004 QTY_LESS_THAN_MIN_QTY
 * Quantity less than min quantity.
 * 数量小于最小值
 
### -4005 QTY_GREATER_THAN_MAX_QTY
 * Quantity greater than max quantity.
 * 数量大于最大值

### -4006 STOP_PRICE_LESS_THAN_ZERO
 * Stop price less than zero. 
 * 触发价小于最小值
 
### -4007 STOP_PRICE_GREATER_THAN_MAX_PRICE
 * Stop price greater than max price.
 * 触发价大于最大值

### -4008 TICK_SIZE_LESS_THAN_ZERO
 * Tick size less than zero.
 * 价格精度小于0

### -4009 MAX_PRICE_LESS_THAN_MIN_PRICE
 * Max price less than min price.
 * 最大价格小于最小价格

### -4010 MAX_QTY_LESS_THAN_MIN_QTY
 * Max qty less than min qty.
 * 最大数量小于最小数量

### -4011 STEP_SIZE_LESS_THAN_ZERO
 * Step size less than zero.
 * 步进值小于0

### -4012 MAX_NUM_ORDERS_LESS_THAN_ZERO
 * Max num orders less than zero.
 * 最大订单量小于0

### -4013 PRICE_LESS_THAN_MIN_PRICE
 * Price less than min price.
 * 价格小于最小价格

### -4014 PRICE_NOT_INCREASED_BY_TICK_SIZE
 * Price not increased by tick size.
 * 价格增量不是价格精度的倍数。
 
### -4015 INVALID_CL_ORD_ID_LEN
 * Client order id is not valid.
 * 客户订单ID有误。
 * Client order id length should not be more than 36 chars
 * 客户订单ID长度应该不多于36字符

### -4016 PRICE_HIGHTER_THAN_MULTIPLIER_UP
 * Price is higher than mark price multiplier cap.

### -4017 MULTIPLIER_UP_LESS_THAN_ZERO
 * Multiplier up less than zero.
 * 价格上限小于0

### -4018 MULTIPLIER_DOWN_LESS_THAN_ZERO
 * Multiplier down less than zero.
 * 价格下限小于0

### -4019 COMPOSITE_SCALE_OVERFLOW
 * Composite scale too large.

### -4020 TARGET_STRATEGY_INVALID
 * Target strategy invalid for orderType '%s',reduceOnly '%b'.
 * 目标策略值不适合`%s`订单状态, 只减仓`%b`。

### -4021 INVALID_DEPTH_LIMIT
 * Invalid depth limit.
 * 深度信息的`limit`值不正确。
 * '%s' is not valid depth limit.
 * `%s`不是合理的深度信息的`limit`值。

### -4022 WRONG_MARKET_STATUS
 * market status sent is not valid.
 * 发送的市场状态不正确。
 
### -4023 QTY_NOT_INCREASED_BY_STEP_SIZE
 * Qty not increased by step size.
 * 数量的递增值不是步进值的倍数。

### -4024 PRICE_LOWER_THAN_MULTIPLIER_DOWN
 * Price is lower than mark price multiplier floor.

### -4025 MULTIPLIER_DECIMAL_LESS_THAN_ZERO
 * Multiplier decimal less than zero.

### -4026 COMMISSION_INVALID
 * Commission invalid.
 * 收益值不正确
 * `%s` less than zero.
 * `%s`少于0
 * `%s` absolute value greater than `%s`
 * `%s`绝对值大于`%s`

### -4027 INVALID_ACCOUNT_TYPE
 * Invalid account type.
 * 账户类型不正确。

### -4029 INVALID_TICK_SIZE_PRECISION
 * Tick size precision is invalid.
 * 价格精度小数点位数不正确。

### -4030 INVALID_STEP_SIZE_PRECISION
 * Step size precision is invalid.
 * 步进值小数点位数不正确。

### -4031 INVALID_WORKING_TYPE
 * Invalid parameter working type
 * 不正确的参数类型
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

### -4032 EXCEED_MAX_CANCEL_ORDER_SIZE
 * Exceed maximum cancel order size.
 * 超过可以取消的最大订单量。
 * Invalid parameter working type: `%s`
 * 不正确的参数类型: `%s`

### -4044 INVALID_BALANCE_TYPE
 * Balance Type is invalid.
 * 余额类型不正确。

### -4045 MAX_STOP_ORDER_EXCEEDED
 * Reach max stop order limit.
 * 达到止损单的上限。

### -4055 AMOUNT_MUST_BE_POSITIVE
 * Amount must be positive.
 * 数量必须是正整数

### -4056 INVALID_API_KEY_TYPE
 * Invalid api key type.
 * API key的类型不正确

### -4057 INVALID_RSA_PUBLIC_KEY
 * Invalid api public key
 * API key不正确

### -4058 MAX_PRICE_TOO_LARGE
 * maxPrice and priceDecimal too large,please check.
 * maxPrice和priceDecimal太大，请检查。

### -4060 INVALID_POSITION_SIDE
 * Invalid position side.
 * 仓位方向不正确。

### -4061 POSITION_SIDE_NOT_MATCH
 * Order's position side does not match user's setting.
 * 订单的持仓方向和用户设置不一致。

### -4062 REDUCE_ONLY_CONFLICT
 * Invalid or improper reduceOnly value.
 * 仅减仓的设置不正确。

### -4084 UPCOMING_METHOD
 * Method is not allowed currently. Upcoming soon.
 * 方法不支持

### -4086 INVALID_PRICE_SPREAD_THRESHOLD
 * Invalid price spread threshold
 * 无效的价差阀值
 
### -4087 REDUCE_ONLY_ORDER_PERMISSION
 * User can only place reduce only order
 * 用户只能下仅减仓订单

### -4088 NO_PLACE_ORDER_PERMISSION
 * User can not place order currently
 * 用户当前不能下单

### -4114 INVALID_CLIENT_TRAN_ID_LEN
 * clientTranId  is not valid
 * clientTranId不正确
 * Client tran id length should be less than 64 chars
 * 客户的tranId长度应该小于64个字符

### -4115 DUPLICATED_CLIENT_TRAN_ID
 * clientTranId  is duplicated
 *  clientTranId重复
 * Client tran id should be unique within 7 days
 * 客户的tranId应在7天内唯一

### -4118 REDUCE_ONLY_MARGIN_CHECK_FAILED
 * ReduceOnly Order Failed. Please check your existing position and open orders
 * 仅减仓订单失败。请检查现有的持仓和挂单
 
### -4131 MARKET_ORDER_REJECT
 * The counterparty's best price does not meet the PERCENT_PRICE filter limit
 * 交易对手的最高价格未达到PERCENT_PRICE过滤器限制

### -4135 INVALID_ACTIVATION_PRICE
 * Invalid activation price
 * 无效的激活价格

### -4137 QUANTITY_EXISTS_WITH_CLOSE_POSITION
 * Quantity must be zero with closePosition equals true
 * 数量必须为0，当closePosition为true时

### -4138 REDUCE_ONLY_MUST_BE_TRUE
 * Reduce only must be true with closePosition equals true
 * Reduce only 必须为true，当closePosition为true时

### -4139 ORDER_TYPE_CANNOT_BE_MKT
 * Order type can not be market if it's unable to cancel
 * 订单类型不能为市价单如果不能取消

### -4140 INVALID_OPENING_POSITION_STATUS
 * Invalid symbol status for opening position
 * 无效的交易对状态

### -4141 SYMBOL_ALREADY_CLOSED
 * Symbol is closed
 * 交易对已下架

### -4142 STRATEGY_INVALID_TRIGGER_PRICE
 * REJECT: take profit or stop order will be triggered immediately
 * 拒绝：止盈止损单将立即被触发

### -4164 MIN_NOTIONAL
 * Order's notional must be no smaller than 5.0 (unless you choose reduce only)
 *  订单的名义价值不可以小于5，除了使用reduce only
 * Order's notional must be no smaller than %s (unless you choose reduce only)
 *  订单的名义价值不可以小于`%s`，除了使用reduce only

### -4165 INVALID_TIME_INTERVAL
 * Invalid time interval
 * 无效的间隔
 * Maximum time interval is %s days
 * 最大的时间间隔为 `%s` 天

### -4183 PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP
 * Price is higher than stop price multiplier cap.
 * 止盈止损订单价格不应高于触发价与报价乘数上限的乘积
 * Limit price can't be higher than %s.
 * 止盈止损订单价格不应高于 `%s`

### -4184 PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN
 * Price is lower than stop price multiplier floor.
 * 止盈止损订单价格不应低于触发价与报价乘数下限的乘积
 * Limit price can't be lower than %s.
 * 止盈止损订单价格不应低于 `%s`f
