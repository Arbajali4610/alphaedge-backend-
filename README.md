# AlphaEdge Backend

Express + SQLite backend for the AlphaEdge frontend.

## What it provides
- Client registration with server-side SQLite storage.
- Password hashing with bcryptjs; plaintext passwords are never stored.
- Client ID generation (`AE100001`, `AE100002`, ...).
- Login/logout using an HTTP-only session cookie.
- Forgot Client ID / Forgot Password endpoints.
- Profile update endpoint.
- Payment confirmation storage, including uploaded slip files.
- Upstox Market Data Feed V3 bridge to `/api/market` and `/api/market-stream`.
- Real-time NIFTY 50, SENSEX and BANK NIFTY updates pushed to the existing frontend via Server-Sent Events.

## Setup
1. Install Node.js 20+.
2. Copy `.env.example` to `.env`.
3. Put your Upstox **access token** in `UPSTOX_ACCESS_TOKEN`.
4. `npm install`
5. `npm start`

The backend serves the frontend from `../frontend` when this package is run from the full-stack folder.

## Important
GitHub Pages is static hosting and cannot run this backend. Deploy the backend on a Node-capable service (Render, Railway, VPS, etc.) and point the frontend API URL to that server if frontend/backend are on different domains.

Never commit `.env`, an Upstox secret, access token, client secret, database, or uploaded payment slips to GitHub.
