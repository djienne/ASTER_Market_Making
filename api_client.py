import json
import math
import time
import urllib.parse

import aiohttp
from eth_account import Account
try:
    from eth_account.messages import encode_typed_data as _encode_typed_message
except ImportError:  # pragma: no cover - compatibility path
    from eth_account.messages import encode_structured_data as _encode_typed_message
from web3 import Web3


def _trim_dict(my_dict):
    """Helper function to convert all dictionary values to strings recursively."""
    for key, value in my_dict.items():
        if isinstance(value, list):
            new_value = [json.dumps(_trim_dict(item)) if isinstance(item, dict) else str(item) for item in value]
            my_dict[key] = json.dumps(new_value)
        elif isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
        else:
            my_dict[key] = str(value)
    return my_dict


class ApiClient:
    """
    An asynchronous client for interacting with the Aster Finance API,
    handling session management and request signing.
    """

    def __init__(self, api_user, api_signer, api_private_key, release_mode=True):
        if not api_user or not Web3.is_address(api_user):
            raise ValueError("API_USER is missing or not a valid Ethereum address.")
        if not api_signer or not Web3.is_address(api_signer):
            raise ValueError("API_SIGNER is missing or not a valid Ethereum address.")
        if not api_private_key:
            raise ValueError("API_PRIVATE_KEY is missing.")

        self.api_user = api_user
        self.api_signer = api_signer
        self.api_private_key = api_private_key
        self.release_mode = release_mode

        self.base_url = "https://fapi.asterdex.com"
        self.session = None
        self.timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_connect=10, sock_read=20)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _sign(self, params):
        """Sign request params using the official Pro API V3 typed-data signature flow."""
        nonce = math.trunc(time.time() * 1000000)
        my_dict = {k: v for k, v in params.items() if v is not None}
        my_dict["nonce"] = str(nonce)
        my_dict["user"] = self.api_user
        my_dict["signer"] = self.api_signer
        _trim_dict(my_dict)
        payload = urllib.parse.urlencode(my_dict)

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [
                    {"name": "msg", "type": "string"},
                ],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": payload},
        }
        try:
            signable_msg = _encode_typed_message(full_message=typed_data)
        except TypeError:
            signable_msg = _encode_typed_message(typed_data)
        signed_message = Account.sign_message(signable_msg, private_key=self.api_private_key)

        my_dict['signature'] = signed_message.signature.hex()
        return my_dict

    def _build_headers(self):
        return {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0',
        }

    def _prepare_request(self, params: dict = None):
        clean_params = dict(params or {})
        request_params = self._sign(clean_params)
        headers = self._build_headers()
        return request_params, headers

    async def _request_json(self, method: str, url: str, request_params: dict, headers: dict):
        request_specs = {
            'GET': (self.session.get, 'params'),
            'POST': (self.session.post, 'data'),
            'PUT': (self.session.put, 'data'),
            'DELETE': (self.session.delete, 'data'),
        }

        method = method.upper()
        if method not in request_specs:
            raise ValueError(f"Unsupported HTTP method: {method}")

        request_fn, payload_key = request_specs[method]
        request_kwargs = {payload_key: request_params, 'headers': headers}

        async with request_fn(url, **request_kwargs) as response:
            if not response.ok:
                error_body = await response.text()
                if not self.release_mode:
                    print(f"API Error on {method} {url}: Status={response.status}, Body={error_body}")
            response.raise_for_status()
            return await response.json()

    async def get_exchange_info(self):
        """Get exchange information from the public endpoint."""
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def get_symbol_filters(self, symbol: str) -> dict:
        exchange_info = await self.get_exchange_info()
        for sym_data in exchange_info.get('symbols', []):
            if sym_data['symbol'] == symbol:
                filters = {f['filterType']: f for f in sym_data.get('filters', [])}
                price_filter = filters.get('PRICE_FILTER', {})
                tick_size_str = price_filter.get('tickSize', '0.01')
                price_precision = len(tick_size_str.split('.')[1].rstrip('0')) if '.' in tick_size_str else 0
                lot_size_filter = filters.get('LOT_SIZE', {})
                step_size_str = lot_size_filter.get('stepSize', '0.01')
                min_qty_str = lot_size_filter.get('minQty', step_size_str)
                quantity_precision = len(step_size_str.split('.')[1].rstrip('0')) if '.' in step_size_str else 0
                return {
                    'status': sym_data.get('status', 'UNKNOWN'),
                    'price_precision': price_precision,
                    'tick_size': float(tick_size_str),
                    'quantity_precision': quantity_precision,
                    'step_size': float(step_size_str),
                    'min_qty': float(min_qty_str),
                    'min_notional': float(filters.get('MIN_NOTIONAL', {}).get('notional', '5.0'))
                }
        raise ValueError(f"Could not find filters for symbol '{symbol}'.")

    async def signed_request(self, method: str, endpoint: str, params: dict = None):
        """Generic method for making signed requests to the Pro API V3."""
        url = f"{self.base_url}{endpoint}"
        request_params, headers = self._prepare_request(params)
        return await self._request_json(method, url, request_params, headers)

    async def place_order(self, symbol, price, quantity, side, reduce_only=False):
        """Place a limit post-only order using Ethereum signature auth."""
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTX",
            "price": price,
            "quantity": quantity,
            "positionSide": "BOTH",
        }
        if reduce_only:
            params['reduceOnly'] = 'true'
        return await self.signed_request("POST", "/fapi/v3/order", params)

    async def get_order_status(self, symbol, order_id):
        """Get order status using Ethereum signature auth."""
        params = {"symbol": symbol, "orderId": order_id}
        return await self.signed_request("GET", "/fapi/v3/order", params)

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        """Cancel an order using Ethereum signature auth."""
        params = {"symbol": symbol, "orderId": order_id}
        return await self.signed_request("DELETE", "/fapi/v3/order", params)

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all orders for a symbol using Ethereum signature auth."""
        params = {"symbol": symbol}
        return await self.signed_request("DELETE", "/fapi/v3/allOpenOrders", params)

    async def get_open_orders(self, symbol: str = None):
        """Get all open orders, optionally scoped to a symbol."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self.signed_request("GET", "/fapi/v3/openOrders", params)

    async def get_position_risk(self, symbol: str = None):
        """Get position risk information using Ethereum signature auth."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self.signed_request("GET", "/fapi/v3/positionRisk", params)

    async def change_leverage(self, symbol: str, leverage: int):
        """Change the initial leverage for a symbol."""
        params = {"symbol": symbol, "leverage": leverage}
        return await self.signed_request("POST", "/fapi/v3/leverage", params)

    async def create_listen_key(self):
        """Create or extend a Pro API V3 user-data listen key."""
        return await self.signed_request("POST", "/fapi/v3/listenKey", {})

    async def keepalive_listen_key(self):
        """Keep a Pro API V3 user-data listen key alive."""
        return await self.signed_request("PUT", "/fapi/v3/listenKey", {})

    async def close_listen_key(self):
        """Close a Pro API V3 user-data listen key."""
        return await self.signed_request("DELETE", "/fapi/v3/listenKey", {})
