const express = require('express');
const UpstoxClient = require('upstox-js-sdk');
const bcrypt = require('bcryptjs');
const session = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const { Pool } = require('pg');

const app = express();

const PORT = process.env.PORT || 10000;
const ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;

app.set('trust proxy', 1);

app.use(express.json({ limit: '100kb' }));
app.use(express.urlencoded({ extended: true, limit: '100kb' }));

/* =========================
DATABASE
========================= */

let pool = null;
let databaseReady = false;

if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: {
      rejectUnauthorized: false
    },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  });

  pool.on('error', (err) => {
    console.error('PostgreSQL pool error:', err.message);
  });
}

async function initDatabase() {
  if (!pool) {
    console.warn('DATABASE_URL is not configured. Authentication is disabled.');
    return;
  }

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS clients (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(30) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        phone VARCHAR(20) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS payment_confirmations (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(30) NOT NULL
          REFERENCES clients(client_id) ON DELETE CASCADE,
        course VARCHAR(120) NOT NULL,
        amount NUMERIC(10,2) NOT NULL DEFAULT 999.00,
        utr VARCHAR(100) NOT NULL,
        slip_name VARCHAR(255),
        slip_data TEXT,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS market_snapshots (
        id BIGSERIAL PRIMARY KEY,
        symbol VARCHAR(30) NOT NULL,
        ltp NUMERIC(14,4) NOT NULL,
        close NUMERIC(14,4),
        change_pct NUMERIC(10,4),
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS market_snapshots_symbol_time_idx
      ON market_snapshots(symbol, captured_at DESC);
    `);

    databaseReady = true;
    console.log('PostgreSQL database ready.');

  } catch (err) {
    databaseReady = false;
    console.error(
      'PostgreSQL initialization failed:',
      err.message
    );
  }
}

/* =========================
SESSION
========================= */

if (pool) {
  app.use(
    session({
      store: new pgSession({
        pool,
        tableName: 'user_sessions',
        createTableIfMissing: true
      }),

      secret:
        process.env.SESSION_SECRET ||
        'CHANGE_THIS_SESSION_SECRET',

      resave: false,
      saveUninitialized: false,

      cookie: {
        httpOnly: true,
        secure: process.env.NODE_ENV === 'production',
        sameSite: 'lax',
        maxAge: 1000 * 60 * 60 * 24 * 7
      }
    })
  );
} else {
  app.use(
    session({
      secret:
        process.env.SESSION_SECRET ||
        'CHANGE_THIS_SESSION_SECRET',

      resave: false,
      saveUninitialized: false
    })
  );
}

/* =========================
MARKET DATA
========================= */

const INSTRUMENTS = {
  nifty: 'NSE_INDEX|Nifty 50',
  sensex: 'BSE_INDEX|SENSEX',
  banknifty: 'NSE_INDEX|Nifty Bank'
};

let latest = {
  nifty: null,
  sensex: null,
  banknifty: null,
  connected: false,
  updatedAt: null,
  error: ACCESS_TOKEN
    ? null
    : 'UPSTOX_ACCESS_TOKEN is not configured'
};

async function saveMarketSnapshot(name, value) {
  if (!pool || !databaseReady || !value) return;

  try {
    await pool.query(
      `INSERT INTO market_snapshots
       (symbol, ltp, close, change_pct)
       VALUES ($1, $2, $3, $4)`,
      [
        name,
        value.ltp,
        value.close,
        value.changePct
      ]
    );

  } catch (err) {
    console.error(
      'Market snapshot save failed:',
      err.message
    );
  }
}

const clients = new Set();

app.use(express.static(__dirname));

app.get('/api/market', (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json(latest);
});

app.get('/api/market/history', async (req, res) => {

  if (!pool || !databaseReady) {
    return res.json({
      success: true,
      history: []
    });
  }

  const symbol =
    String(req.query.symbol || 'nifty')
      .toLowerCase();

  const allowed = [
    'nifty',
    'sensex',
    'banknifty'
  ];

  if (!allowed.includes(symbol)) {
    return res.status(400).json({
      success: false,
      message: 'Invalid symbol'
    });
  }

  try {

    const result = await pool.query(
      `SELECT
        ltp,
        close,
        change_pct,
        captured_at
       FROM market_snapshots
       WHERE symbol = $1
       ORDER BY captured_at DESC
       LIMIT 120`,
      [symbol]
    );

    res.set(
      'Cache-Control',
      'no-store'
    );

    res.json({
      success: true,
      history: result.rows.reverse()
    });

  } catch (err) {

    res.status(500).json({
      success: false,
      message: 'Unable to load market history'
    });
  }
});

app.get('/api/market-stream', (req, res) => {

  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control':
      'no-cache, no-store, must-revalidate',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  res.flushHeaders();

  res.write(
    `data: ${JSON.stringify(latest)}\n\n`
  );

  clients.add(res);

  req.on('close', () => {
    clients.delete(res);
  });
});

function broadcast() {

  const payload =
    `data: ${JSON.stringify(latest)}\n\n`;

  for (const res of clients) {

    try {
      res.write(payload);
    } catch (_) {
      clients.delete(res);
    }
  }
}

function extractLtpc(feed) {

  if (!feed || typeof feed !== 'object') {
    return null;
  }

  if (feed.ltpc) {
    return feed.ltpc;
  }

  if (feed.ff?.indexFF?.ltpc) {
    return feed.ff.indexFF.ltpc;
  }

  if (feed.fullFeed?.indexFF?.ltpc) {
    return feed.fullFeed.indexFF.ltpc;
  }

  if (feed.fullFeed?.marketFF?.ltpc) {
    return feed.fullFeed.marketFF.ltpc;
  }

  if (feed.indexFF?.ltpc) {
    return feed.indexFF.ltpc;
  }

  return null;
}

function applyFeed(message) {

  let data = message;

  try {

    if (Buffer.isBuffer(data)) {
      data = data.toString('utf8');
    }

    if (typeof data === 'string') {
      data = JSON.parse(data);
    }

  } catch (_) {
    return;
  }

  const feeds =
    data?.feeds ||
    data?.data?.feeds;

  if (
    !feeds ||
    typeof feeds !== 'object'
  ) {
    return;
  }

  for (
    const [key, feed]
    of Object.entries(feeds)
  ) {

    let name = null;

    if (key === INSTRUMENTS.nifty) {
      name = 'nifty';
    }

    else if (key === INSTRUMENTS.sensex) {
      name = 'sensex';
    }

    else if (key === INSTRUMENTS.banknifty) {
      name = 'banknifty';
    }

    if (!name) continue;

    const ltpc =
      extractLtpc(feed);

    if (
      !ltpc ||
      typeof ltpc.ltp !== 'number'
    ) {
      continue;
    }

    const previousClose =
      Number(ltpc.cp);

    const changePct =
      Number.isFinite(previousClose) &&
      previousClose !== 0
        ? (
            (ltpc.ltp - previousClose) /
            previousClose
          ) * 100
        : null;

    latest[name] = {
      ltp: ltpc.ltp,

      close:
        Number.isFinite(previousClose)
          ? previousClose
          : null,

      changePct,

      ltt:
        ltpc.ltt || null
    };

    latest.updatedAt =
      Date.now();

    latest.error = null;

    saveMarketSnapshot(
      name,
      latest[name]
    );
  }

  broadcast();
}

function startUpstox() {

  if (!ACCESS_TOKEN) {

    console.warn(
      'Live market feed disabled: set UPSTOX_ACCESS_TOKEN in Render Environment Variables.'
    );

    return;
  }

  try {

    const defaultClient =
      UpstoxClient.ApiClient.instance;

    const oauth =
      defaultClient.authentications['OAUTH2'];

    oauth.accessToken =
      ACCESS_TOKEN;

    const streamer =
      new UpstoxClient.MarketDataStreamerV3(
        [
          INSTRUMENTS.nifty,
          INSTRUMENTS.sensex,
          INSTRUMENTS.banknifty
        ],
        'ltpc'
      );

    streamer.on('open', () => {

      latest.connected = true;
      latest.error = null;

      broadcast();

      try {

        streamer.subscribe(
          [
            INSTRUMENTS.nifty,
            INSTRUMENTS.sensex,
            INSTRUMENTS.banknifty
          ],
          'ltpc'
        );

      } catch (err) {

        latest.error =
          `Subscription error: ${err.message}`;

        broadcast();
      }
    });

    streamer.on(
      'message',
      applyFeed
    );

    streamer.on(
      'error',
      (err) => {

        latest.connected = false;

        latest.error =
          err?.message ||
          'Market feed connection error';

        broadcast();
      }
    );

    streamer.on(
      'close',
      () => {

        latest.connected = false;

        latest.error =
          'Market feed disconnected';

        broadcast();
      }
    );

    streamer.connect();

  } catch (err) {

    latest.connected = false;

    latest.error =
      err.message ||
      'Unable to start market feed';

    broadcast();

    console.error(err);
  }
}

/* =========================
AUTH HELPERS
========================= */

function normalizeEmail(email) {

  return String(email || '')
    .trim()
    .toLowerCase();
}

function normalizePhone(phone) {

  return String(phone || '')
    .replace(/\s+/g, '')
    .trim();
}

function generateClientId() {

  const random =
    Math.floor(
      100000 +
      Math.random() * 900000
    );

  return `AE${random}`;
}

function requireLogin(
  req,
  res,
  next
) {

  if (!req.session?.clientId) {

    return res.status(401).json({
      success: false,
      message: 'Login required'
    });
  }

  next();
}

/* =========================
CLIENT REGISTRATION
========================= */

app.post(
  '/api/auth/register',
  async (req, res) => {

    if (!pool || !databaseReady) {

      return res.status(503).json({
        success: false,
        message:
          'Authentication database is unavailable'
      });
    }

    try {

      const name =
        String(req.body.name || '')
          .trim();

      const phone =
        normalizePhone(
          req.body.phone
        );

      const email =
        normalizeEmail(
          req.body.email
        );

      const password =
        String(
          req.body.password || ''
        );

      if (
        !name ||
        !phone ||
        !email ||
        !password
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Name, phone, email and password are required'
        });
      }

      if (name.length > 100) {

        return res.status(400).json({
          success: false,
          message: 'Name is too long'
        });
      }

      if (password.length < 8) {

        return res.status(400).json({
          success: false,
          message:
            'Password must contain at least 8 characters'
        });
      }

      if (
        !/^[^\s@]+@[^\s@]+\.[^\s@]+$/
          .test(email)
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Invalid email address'
        });
      }

      const existing =
        await pool.query(
          `SELECT client_id
           FROM clients
           WHERE email = $1
           OR phone = $2
           LIMIT 1`,
          [email, phone]
        );

      if (
        existing.rows.length > 0
      ) {

        return res.status(409).json({
          success: false,
          message:
            'Email or phone number is already registered'
        });
      }

      let clientId;

      for (
        let i = 0;
        i < 10;
        i++
      ) {

        const candidate =
          generateClientId();

        const check =
          await pool.query(
            `SELECT id
             FROM clients
             WHERE client_id = $1`,
            [candidate]
          );

        if (
          check.rows.length === 0
        ) {

          clientId =
            candidate;

          break;
        }
      }

      if (!clientId) {

        return res.status(500).json({
          success: false,
          message:
            'Unable to create Client ID'
        });
      }

      const passwordHash =
        await bcrypt.hash(
          password,
          12
        );

      await pool.query(
        `INSERT INTO clients
         (
           client_id,
           name,
           phone,
           email,
           password_hash
         )
         VALUES
         ($1, $2, $3, $4, $5)`,
        [
          clientId,
          name,
          phone,
          email,
          passwordHash
        ]
      );

      return res.status(201).json({
        success: true,
        message:
          'Registration successful',
        clientId
      });

    } catch (err) {

      console.error(
        'Registration error:',
        err.message
      );

      return res.status(500).json({
        success: false,
        message:
          'Registration failed'
      });
    }
  }
);

/* =========================
CLIENT LOGIN
========================= */

app.post(
  '/api/auth/login',
  async (req, res) => {

    if (!pool || !databaseReady) {

      return res.status(503).json({
        success: false,
        message:
          'Authentication database is unavailable'
      });
    }

    try {

      const clientId =
        String(
          req.body.clientId || ''
        ).trim();

      const password =
        String(
          req.body.password || ''
        );

      if (
        !clientId ||
        !password
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Client ID and password are required'
        });
      }

      const result =
        await pool.query(
          `SELECT
             id,
             client_id,
             name,
             phone,
             email,
             password_hash,
             status
           FROM clients
           WHERE client_id = $1
           LIMIT 1`,
          [clientId]
        );

      if (
        result.rows.length === 0
      ) {

        return res.status(401).json({
          success: false,
          message:
            'Invalid Client ID or password'
        });
      }

      const client =
        result.rows[0];

      if (
        client.status !== 'active'
      ) {

        return res.status(403).json({
          success: false,
          message:
            'Account is not active'
        });
      }

      const passwordCorrect =
        await bcrypt.compare(
          password,
          client.password_hash
        );

      if (!passwordCorrect) {

        return res.status(401).json({
          success: false,
          message:
            'Invalid Client ID or password'
        });
      }

      req.session.clientId =
        client.client_id;

      req.session.userType =
        'client';

      return res.json({
        success: true,

        client: {
          clientId:
            client.client_id,

          name:
            client.name,

          email:
            client.email
        }
      });

    } catch (err) {

      console.error(
        'Login error:',
        err.message
      );

      return res.status(500).json({
        success: false,
        message:
          'Login failed'
      });
    }
  }
);

/* =========================
CURRENT USER
========================= */

app.get(
  '/api/auth/me',
  requireLogin,
  async (req, res) => {

    try {

      const result =
        await pool.query(
          `SELECT
             client_id,
             name,
             email,
             phone,
             status,
             created_at
           FROM clients
           WHERE client_id = $1
           LIMIT 1`,
          [
            req.session.clientId
          ]
        );

      if (
        result.rows.length === 0
      ) {

        req.session.destroy(
          () => {}
        );

        return res.status(401).json({
          success: false,
          message:
            'Account not found'
        });
      }

      const client =
        result.rows[0];

      return res.json({
        success: true,

        client: {
          clientId:
            client.client_id,

          name:
            client.name,

          email:
            client.email,

          phone:
            client.phone,

          status:
            client.status,

          createdAt:
            client.created_at
        }
      });

    } catch (err) {

      console.error(
        'Session lookup error:',
        err.message
      );

      return res.status(500).json({
        success: false,
        message:
          'Unable to load account'
      });
    }
  }
);

/* =========================
PROFILE UPDATE
========================= */

app.put(
  '/api/auth/profile',
  requireLogin,
  async (req, res) => {

    try {

      const name =
        String(
          req.body.name || ''
        ).trim();

      const phone =
        normalizePhone(
          req.body.phone
        );

      const email =
        normalizeEmail(
          req.body.email
        );

      const password =
        String(
          req.body.password || ''
        );

      if (
        !name ||
        !/^\d{10}$/.test(phone) ||
        !/^\S+@\S+\.\S+$/.test(email)
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Valid name, mobile and email are required'
        });
      }

      if (
        password &&
        password.length < 8
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Password must contain at least 8 characters'
        });
      }

      const fields = [
        'name=$1',
        'phone=$2',
        'email=$3'
      ];

      const vals = [
        name,
        phone,
        email
      ];

      if (password) {

        fields.push(
          'password_hash=$4'
        );

        vals.push(
          await bcrypt.hash(
            password,
            12
          )
        );
      }

      vals.push(
        req.session.clientId
      );

      const result =
        await pool.query(
          `UPDATE clients
           SET ${fields.join(', ')}
           WHERE client_id=$${vals.length}
           RETURNING
             client_id,
             name,
             phone,
             email`,
          vals
        );

      if (
        !result.rows.length
      ) {

        return res.status(404).json({
          success: false,
          message:
            'Account not found'
        });
      }

      res.json({
        success: true,
        client:
          result.rows[0]
      });

    } catch (err) {

      console.error(
        'Profile update error:',
        err.message
      );

      res.status(500).json({
        success: false,
        message:
          'Profile update failed'
      });
    }
  }
);

/* =========================
PAYMENT CONFIRMATION
========================= */

app.post(
  '/api/payments/confirm',
  requireLogin,
  async (req, res) => {

    if (
      !pool ||
      !databaseReady
    ) {

      return res.status(503).json({
        success: false,
        message:
          'Database unavailable'
      });
    }

    try {

      const course =
        String(
          req.body.course || ''
        ).trim();

      const utr =
        String(
          req.body.utr || ''
        ).trim();

      const slipName =
        String(
          req.body.slipName || ''
        ).trim();

      const slipData =
        String(
          req.body.slipData || ''
        );

      if (
        !course ||
        !utr ||
        !slipName ||
        !slipData
      ) {

        return res.status(400).json({
          success: false,
          message:
            'Payment details are required'
        });
      }

      const result =
        await pool.query(
          `INSERT INTO payment_confirmations
           (
             client_id,
             course,
             amount,
             utr,
             slip_name,
             slip_data
           )
           VALUES
           ($1, $2, 999, $3, $4, $5)
           RETURNING id, created_at`,
          [
            req.session.clientId,
            course,
            utr,
            slipName,
            slipData
          ]
        );

      res.status(201).json({
        success: true,
        paymentId:
          result.rows[0].id,

        createdAt:
          result.rows[0].created_at
      });

    } catch (err) {

      console.error(
        'Payment confirmation error:',
        err.message
      );

      res.status(500).json({
        success: false,
        message:
          'Payment confirmation failed'
      });
    }
  }
);

/* =========================
LOGOUT
========================= */

app.post(
  '/api/auth/logout',
  (req, res) => {

    req.session.destroy(
      (err) => {

        if (err) {

          return res.status(500).json({
            success: false,
            message:
              'Logout failed'
          });
        }

        res.clearCookie(
          'connect.sid'
        );

        return res.json({
          success: true,
          message:
            'Logged out successfully'
        });
      }
    );
  }
);

/* =========================
HEALTH CHECK
========================= */

app.get(
  '/api/health',
  (req, res) => {

    res.json({

      success: true,

      server:
        'online',

      database:
        databaseReady
          ? 'connected'
          : 'not-connected',

      marketFeed:
        latest.connected
          ? 'connected'
          : 'disconnected',

      time:
        new Date().toISOString()
    });
  }
);

/* =========================
START SERVER
========================= */

app.listen(
  PORT,
  async () => {

    console.log(
      `AlphaEdge running on port ${PORT}`
    );

    await initDatabase();

    startUpstox();
  }
);
