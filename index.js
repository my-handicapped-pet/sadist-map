import { config } from "dotenv";

import express from "express";

import { MongoClient } from "mongodb";

import * as assert from "node:assert";
import pino from "pino";
import swaggerUi from "swagger-ui-express";
import YAML from 'yamljs';
import { circle, featureCollection, intersect, simplify } from "@turf/turf";

const logger = pino();

config();
const app = express();
app.use(express.json({ limit: '100mb' }));

// --- Swagger setup ---
const swaggerDocument = YAML.load('./swagger.yml');
app.use('/map/swagger', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// --- Ensure geo-spacial index (for a given collection) ---
async function ensureGeospatialIndexForCollection(collectionName) {
  await ctx.db.collection(collectionName).createIndex({ geometry: '2dsphere' });
  logger.info(`${collectionName}: 2dsphere index ready.`);
}

// --- Ensure geo-spacial index (for all collections) ---
async function ensureGeospatialIndex() {
  const collectionList = await ctx.db.listCollections().toArray();

  await Promise.all(
      collectionList.filter((collection) => collection.name.startsWith('geo_'))
          .map(async (collection) => {
            await ensureGeospatialIndexForCollection(collection.name);
          })
  );
}

// --- Connect to MongoDB ---
async function connectDB() {
  const uri = process.env.DATABASE_URL;

  const client = new MongoClient(uri, { authSource: 'admin' });
  await client.connect();
  return client.db();
}

const ctx = {
  /**
   * Db connection
   * @type {import('mongodb').Db}
   */
  db: null,

  test() {
    assert(this.db, "Db isn\'t ready");
  }
}
connectDB()
    .then(async (db) => {
      ctx.db = db;
      await ensureGeospatialIndex();
    })
    .then(() => {
      // now we can start application
      const port = process.env.APP_PORT || 3000;
      app.listen(port, () => {
        logger.info(`GeoJSON API running on http://localhost:${port}`);
      });
    })
    .catch((err) => {
      logger.fatal(err);
    });

// --- Basic auth middleware ---
let basicAuth;
if (!process.env.APP_LOGIN || !process.env.APP_PASSWORD) {
  // If login / password are not set, we won't create editing endpoints.
  // E.g. if on the production server environment variables are missing,
  // the editing endpoints won't be created.
  // But get endpoint will still be created.
  basicAuth = null;
} else {
  basicAuth = function (req, res, next) {
    // Token is sent in Base64
    const authToken = req.headers.authorization;
    if (!authToken || !authToken.startsWith('Basic ')) {
      const err = new Error('Unauthorized');
      err.status = 401;
      throw err;
    }

    const decodedToken = Buffer.from(authToken.split(' ')[1], 'base64').toString('utf8');
    const [user, password] = decodedToken.split(':');

    if (
        user !== process.env.APP_LOGIN ||
        password !== process.env.APP_PASSWORD
    ) {
      const err = new Error('Forbidden');
      err.status = 403;
      throw err;
    }

    next();
  }
}

// --- Create a rule ---
if (basicAuth) app.post("/map/rule/:rule", basicAuth, async (req, res) => {
  const { featureclass, mapping } = req.body;
  const ruleId = req.params.rule;

  if (!featureclass) throw new Error("featureclass is required");
  if (!/^[\w]+$/.test(featureclass)) throw new Error("featureclass must be alphanumerical");
  if (!mapping || typeof mapping !== "object") {
    throw new Error("mapping must be an object");
  }
  if (!( '_id' in mapping )) {
    throw new Error("mapping must contain _id");
  }

  const collection = ctx.db.collection("geo_upload_rule");
  const result = await collection.updateOne(
      { _id: ruleId },
      { $set: { featureclass, mapping } },
      { upsert: true },
  );

  res.json({
    success: true,
    inserted: result.upsertedCount,
    modified: result.modifiedCount,
  });
});

// --- Endpoint to upload GeoJSON feature collection ---
if (basicAuth) app.post('/map/upload/:rule', basicAuth, async (req, res) => {
  const { rule: ruleId } = req.params;

  const { type, features } = req.body;
  if (type !== "FeatureCollection" || !Array.isArray(features)) {
    throw new Error("Body must be a valid GeoJSON FeatureCollection");
  }

  const rule = await ctx.db.collection("geo_upload_rule").findOne({ _id: ruleId });
  if (!rule) {
    const err = new Error(`Upload rule '${ruleId}' not found`);
    err.status = 404;
    throw err;
  }

  const targetCollection = `geo_${rule.featureclass}`;
  const mapping = rule.mapping;
  const collection = ctx.db.collection(targetCollection);

  await ensureGeospatialIndexForCollection(targetCollection);

  const ops = features.map((feature) => {
    if (feature.type !== "Feature") {
      throw new Error("All items must be GeoJSON Features");
    }

    const mappedDoc = {};
    for (const [targetField, sourceField] of Object.entries(mapping)) {
      const val = feature.properties?.[sourceField];
      if (targetField === "_id") {
        if (!val) throw new Error("Feature missing _id value from mapping");
        mappedDoc._id = val;
      } else {
        mappedDoc[targetField] = val;
      }
    }
    mappedDoc.geometry = feature.geometry;

    return {
      updateOne: {
        filter: { _id: mappedDoc._id },
        update: { $set: mappedDoc },
        upsert: true,
      },
    };
  });

  if (ops.length === 0) {
    throw new Error("No valid features to insert");
  }

  const result = await collection.bulkWrite(ops, { ordered: false });

  res.json({
    success: true,
    collection: targetCollection,
    inserted: result.upsertedCount,
    modified: result.modifiedCount,
  });
});

// --- Endpoint to query GeoJSON ---
app.get('/map/features/:featureclass', async (req, res) => {
  const { featureclass } = req.params;
  const lng0 = parseFloat(req.query.lng);
  const lat0 = parseFloat(req.query.lat);
  const radius = parseFloat(req.query.radius) || Math.PI / 2;

  if (isNaN(lng0) || isNaN(lat0)) {
    const error = new Error("Invalid coordinates");
    error.status = 400;
    throw error;
  }

  // Normalize coords.
  // Longitude must always be in [−180, 180).
  // Latitude must always be in [−90, 90], but if you go beyond, you “reflect” across the pole and add 180° to longitude.

  // Algorithm:
  // Treat lon, lat as spherical coordinates:
  // φ = latitude (radians), λ = longitude (radians).
  //
  // Convert to Cartesian:
  // x = cosφ * cosλ, y = cosφ * sinλ, z = sinφ.
  // Then recover normalized coordinates from the unit vector:
  // lat' = asin(z) (radians) → in [-π/2, π/2] (i.e. [-90°, 90°])
  // lon' = atan2(y, x) (radians) → in (-π, π] (i.e. (-180°, 180°])
  const phi = (Math.PI / 180) * lat0;
  const lambda = (Math.PI / 180) * lng0;
  const x = Math.cos(phi) * Math.cos(lambda);
  const y = Math.cos(phi) * Math.sin(lambda);
  const z = Math.sin(phi);
  const lat = Math.asin(z) * (180 / Math.PI);
  const lng = Math.atan2(y, x) * (180 / Math.PI);

  // Radius in m, for $near
  // (given the classical meridian length of 20,000 km)
  const radiusM = ( 20_000_000 / Math.PI ) * radius;

  // Zoom limit, currently not implemented in the db, kept for the future
  const zoom = Math.max(1, Math.floor(-Math.log2(radius / Math.PI)));

  // Area which surrounds the displayed map.
  // const area = circle([lng, lat], radius, {
  //   steps: 64,
  //   units: 'radians',
  // });
  // TODO use the correct area instead

  const collection = ctx.db.collection(`geo_${featureclass}`);
  const docs = await collection
      .find({
        $or: [
          { zoom: { $exists: false } },
          { zoom: { $lte: zoom } },
        ],
        geometry: {
          $near: {
            $geometry: {
              type: "Point",
              coordinates: [lng, lat],
            },
            $maxDistance: radiusM,
          },
        },
      })
      .toArray();

  // Change the coefficient to regulate granularity
  const tolerance = .18 * radius;

  // Map and simplify features
  const features = docs.map((doc) => {
    const rawFeature =  {
      type: "Feature",
      geometry: doc.geometry,
    };

    const simplifiedFeature = simplify(rawFeature, {
      tolerance, highQuality: false,
    });
    // TODO intersect simplifiedFeature with area

    return simplifiedFeature;
  }).filter(Boolean);

  res.json({
    type: "FeatureCollection",
    features,
  });
});

// --- Error handling must be placed after all endpoints ---
app.use((err, req, res, next) => {
  logger.error(err);
  res.status(err.status || 500).json({
    error: err.message || "Internal Server Error",
  });
});
