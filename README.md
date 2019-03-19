# Ratelimiter

A service for helping an application ratelimit its requests to external services.

## Usage

See [`ratelimiter.proto`](https://github.com/appellation/ratelimiter/blob/master/ratelimiter.proto) for a description of the gRPC methods available.

After the first request for an ID, the only required parameter is the ID. Any changes to interval will not be applied until the bucket completely drains. Use the `incr` to consume multiple tokens or return tokens to the bucket; defaults to `-1`.

The return value of the request specifies how long the client should wait before proceeding with its execution.
