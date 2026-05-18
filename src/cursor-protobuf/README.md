# Cursor Protobuf Implementation

This directory contains the Cursor IDE API protobuf encoder/decoder and checksum
generation logic, adapted from the [9router](https://github.com/decolua/9router)
project (MIT License).

## Source

Original files (commit at time of import):

- `protobuf.js` ← `open-sse/utils/cursorProtobuf.js`
- `checksum.js` ← `open-sse/utils/cursorChecksum.js`

## Adaptations

1. Converted ESM (`import`/`export`) to CommonJS (`require`/`module.exports`).
2. Removed dependency on `process.env.CURSOR_PROTOBUF_DEBUG` (still honored).
3. Inlined `uuid` v4/v5 helpers via the existing `uuid` package.

## Endpoint Used

- Path: `/aiserver.v1.ChatService/StreamUnifiedChatWithTools`
- Protocol: ConnectRPC over HTTP/2
- Content-Type: `application/connect+proto` (true protobuf binary)
- Cursor client version: `3.1.0`

## License

MIT (inherited from upstream).
