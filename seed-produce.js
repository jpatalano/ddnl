/**
 * seed-produce.js  —  DDNL Platform / Produce Stand Demo
 *
 * Retail-accurate 4-level hierarchy:
 *   Department → Category → Subcategory → Item (with PLU code)
 *
 * Datasets:
 *   sales       — daily by store/dept/category/subcategory/item
 *   shrinkage   — daily by store/dept/category/subcategory/item/reason
 *   customers   — daily by store/type/channel
 *
 * Usage:
 *   INSTANCE_ID=produce \
 *   DATABASE_URL=postgresql://... \
 *   ELASTICSEARCH_URL=https://... \
 *   node seed-produce.js
 */

'use strict';
const { Client } = require('@elastic/elasticsearch');
const { Pool }   = require('pg');

const INSTANCE_ID = process.env.INSTANCE_ID || 'produce';
const ES_URL      = process.env.ELASTICSEARCH_URL || 'http://localhost:9200';
const DB_URL      = process.env.DATABASE_URL;

const es   = new Client({ node: ES_URL });
const pool = DB_URL ? new Pool({ connectionString: DB_URL }) : null;

// ─── Stores ───────────────────────────────────────────────────────────────────

// Persistent store multipliers — fixed so the same store is always bigger/smaller.
// Range 0.50 → 1.50 gives ~3x spread between top and bottom store.
const STORES = [
  { id: 'store_001', name: 'Downtown Market',      city: 'Austin',       state: 'TX', region: 'Central', store_type: 'Urban',    storeM: 1.50 },  // #1 — flagship urban
  { id: 'store_002', name: 'Westside Fresh',        city: 'Austin',       state: 'TX', region: 'Central', store_type: 'Urban',    storeM: 1.25 },
  { id: 'store_003', name: 'Lakeline Stand',        city: 'Cedar Park',   state: 'TX', region: 'North',   store_type: 'Suburban', storeM: 1.10 },
  { id: 'store_004', name: 'South Congress',        city: 'Austin',       state: 'TX', region: 'South',   store_type: 'Urban',    storeM: 0.95 },
  { id: 'store_005', name: 'Mueller Farmers',       city: 'Austin',       state: 'TX', region: 'East',    store_type: 'Urban',    storeM: 0.80 },
  { id: 'store_006', name: 'Round Rock Fresh',      city: 'Round Rock',   state: 'TX', region: 'North',   store_type: 'Suburban', storeM: 0.65 },
  { id: 'store_007', name: 'Pflugerville Stand',    city: 'Pflugerville', state: 'TX', region: 'North',   store_type: 'Suburban', storeM: 0.50 },  // #7 — smallest
];

// ─── Product Taxonomy ─────────────────────────────────────────────────────────
// Structure: { department, category, subcategory, items: [{ name, plu, unit, base_price }] }
// PLU codes follow IFPS standards where applicable
// unit: 'lb' | 'each' | 'bunch' | 'pkg'

const TAXONOMY = [

  // ══════════════════════════════════════════════
  // DEPARTMENT: Fresh Fruit
  // ══════════════════════════════════════════════

  { department: 'Fresh Fruit', category: 'Citrus', subcategory: 'Oranges', items: [
    { name: 'Navel Orange',       plu: '3107', unit: 'each', base_price: 0.99 },
    { name: 'Blood Orange',       plu: '3655', unit: 'each', base_price: 1.29 },
    { name: 'Cara Cara Orange',   plu: '3143', unit: 'each', base_price: 1.19 },
    { name: 'Valencia Orange',    plu: '4012', unit: 'each', base_price: 0.89 },
  ]},
  { department: 'Fresh Fruit', category: 'Citrus', subcategory: 'Lemons & Limes', items: [
    { name: 'Lemon',              plu: '4053', unit: 'each', base_price: 0.75 },
    { name: 'Meyer Lemon',        plu: '3139', unit: 'each', base_price: 1.09 },
    { name: 'Lime',               plu: '4046', unit: 'each', base_price: 0.49 },
  ]},
  { department: 'Fresh Fruit', category: 'Citrus', subcategory: 'Grapefruit & Tangerines', items: [
    { name: 'Ruby Red Grapefruit',plu: '4295', unit: 'each', base_price: 1.49 },
    { name: 'Tangerine',          plu: '3105', unit: 'each', base_price: 0.89 },
    { name: 'Clementine (bag)',   plu: '3341', unit: 'pkg',  base_price: 5.99 },
  ]},

  { department: 'Fresh Fruit', category: 'Stone Fruit', subcategory: 'Peaches & Nectarines', items: [
    { name: 'Yellow Peach',       plu: '4038', unit: 'each', base_price: 1.29 },
    { name: 'White Peach',        plu: '3035', unit: 'each', base_price: 1.49 },
    { name: 'Yellow Nectarine',   plu: '4378', unit: 'each', base_price: 1.29 },
    { name: 'White Nectarine',    plu: '3036', unit: 'each', base_price: 1.49 },
  ]},
  { department: 'Fresh Fruit', category: 'Stone Fruit', subcategory: 'Plums & Cherries', items: [
    { name: 'Black Plum',         plu: '4042', unit: 'each', base_price: 0.99 },
    { name: 'Pluot',              plu: '3436', unit: 'each', base_price: 1.19 },
    { name: 'Apricot',            plu: '3016', unit: 'each', base_price: 1.09 },
    { name: 'Bing Cherries',      plu: '4479', unit: 'lb',   base_price: 4.99 },
    { name: 'Rainier Cherries',   plu: '3139', unit: 'lb',   base_price: 6.99 },
  ]},

  { department: 'Fresh Fruit', category: 'Berries', subcategory: 'Strawberries & Blueberries', items: [
    { name: 'Strawberries (1lb)', plu: '3051', unit: 'pkg',  base_price: 3.99 },
    { name: 'Blueberries (pint)', plu: '3242', unit: 'pkg',  base_price: 4.49 },
  ]},
  { department: 'Fresh Fruit', category: 'Berries', subcategory: 'Raspberries & Blackberries', items: [
    { name: 'Raspberries (6oz)',  plu: '3336', unit: 'pkg',  base_price: 3.99 },
    { name: 'Blackberries (6oz)', plu: '3295', unit: 'pkg',  base_price: 3.49 },
  ]},

  { department: 'Fresh Fruit', category: 'Melons', subcategory: 'Watermelon', items: [
    { name: 'Seedless Watermelon (whole)', plu: '3421', unit: 'each', base_price: 7.99 },
    { name: 'Watermelon (half)',           plu: '3072', unit: 'each', base_price: 4.49 },
    { name: 'Personal Watermelon',         plu: '4032', unit: 'each', base_price: 3.99 },
  ]},
  { department: 'Fresh Fruit', category: 'Melons', subcategory: 'Cantaloupe & Honeydew', items: [
    { name: 'Cantaloupe',         plu: '4318', unit: 'each', base_price: 3.49 },
    { name: 'Honeydew',           plu: '4319', unit: 'each', base_price: 3.99 },
  ]},

  { department: 'Fresh Fruit', category: 'Tropical & Exotic', subcategory: 'Bananas', items: [
    { name: 'Banana (bunch)',     plu: '4011', unit: 'lb',   base_price: 0.29 },  // #1 power item
    { name: 'Organic Banana',     plu: '94011',unit: 'lb',   base_price: 0.49 },
    { name: 'Plantain',           plu: '4235', unit: 'each', base_price: 0.59 },
  ]},
  { department: 'Fresh Fruit', category: 'Tropical & Exotic', subcategory: 'Tropical Fruits', items: [
    { name: 'Mango (Ataulfo)',    plu: '3116', unit: 'each', base_price: 1.49 },
    { name: 'Mango (Tommy)',      plu: '3114', unit: 'each', base_price: 1.29 },
    { name: 'Pineapple',          plu: '4430', unit: 'each', base_price: 2.99 },
    { name: 'Papaya',             plu: '3166', unit: 'each', base_price: 2.49 },
    { name: 'Kiwi',               plu: '3279', unit: 'each', base_price: 0.89 },
    { name: 'Avocado (Hass)',     plu: '4046', unit: 'each', base_price: 1.49 },
  ]},

  { department: 'Fresh Fruit', category: 'Hard Fruit', subcategory: 'Apples', items: [
    { name: 'Gala Apple',         plu: '4133', unit: 'each', base_price: 1.19 },
    { name: 'Fuji Apple',         plu: '4129', unit: 'each', base_price: 1.29 },
    { name: 'Honeycrisp Apple',   plu: '3283', unit: 'each', base_price: 1.79 },
    { name: 'Granny Smith Apple', plu: '4139', unit: 'each', base_price: 1.09 },
    { name: 'Pink Lady Apple',    plu: '3283', unit: 'each', base_price: 1.59 },
  ]},
  { department: 'Fresh Fruit', category: 'Hard Fruit', subcategory: 'Pears', items: [
    { name: 'Bartlett Pear',      plu: '4408', unit: 'each', base_price: 1.29 },
    { name: 'D\'Anjou Pear',      plu: '4409', unit: 'each', base_price: 1.19 },
    { name: 'Bosc Pear',          plu: '4411', unit: 'each', base_price: 1.39 },
  ]},

  // ══════════════════════════════════════════════
  // DEPARTMENT: Fresh Vegetables
  // ══════════════════════════════════════════════

  { department: 'Fresh Vegetables', category: 'Leafy Greens & Salads', subcategory: 'Lettuce', items: [
    { name: 'Iceberg Lettuce',    plu: '4069', unit: 'each', base_price: 1.79 },
    { name: 'Romaine Heart (3pk)',plu: '3237', unit: 'pkg',  base_price: 3.49 },
    { name: 'Green Leaf Lettuce', plu: '4065', unit: 'each', base_price: 1.99 },
    { name: 'Butter Lettuce',     plu: '3492', unit: 'each', base_price: 2.49 },
  ]},
  { department: 'Fresh Vegetables', category: 'Leafy Greens & Salads', subcategory: 'Spinach & Kale', items: [
    { name: 'Baby Spinach (5oz)', plu: '3082', unit: 'pkg',  base_price: 3.99 },
    { name: 'Curly Kale (bunch)', plu: '4467', unit: 'bunch',base_price: 2.49 },
    { name: 'Lacinato Kale',      plu: '3084', unit: 'bunch',base_price: 2.99 },
    { name: 'Swiss Chard',        plu: '4576', unit: 'bunch',base_price: 2.49 },
    { name: 'Arugula (5oz)',      plu: '3481', unit: 'pkg',  base_price: 3.49 },
  ]},
  { department: 'Fresh Vegetables', category: 'Leafy Greens & Salads', subcategory: 'Pre-cut Salads', items: [
    { name: 'Caesar Salad Kit',   plu: '3411', unit: 'pkg',  base_price: 4.99 },
    { name: 'Spring Mix (5oz)',   plu: '3413', unit: 'pkg',  base_price: 3.99 },
    { name: 'Coleslaw Mix',       plu: '3415', unit: 'pkg',  base_price: 2.99 },
  ]},

  { department: 'Fresh Vegetables', category: 'Root Vegetables', subcategory: 'Potatoes', items: [
    { name: 'Russet Potato (5lb)',plu: '4072', unit: 'pkg',  base_price: 4.99 },  // power item
    { name: 'Yukon Gold Potato',  plu: '4060', unit: 'lb',   base_price: 1.49 },
    { name: 'Red Potato (3lb)',   plu: '4073', unit: 'pkg',  base_price: 3.99 },
    { name: 'Sweet Potato',       plu: '4816', unit: 'lb',   base_price: 1.29 },
    { name: 'Fingerling Potato',  plu: '3430', unit: 'lb',   base_price: 2.49 },
  ]},
  { department: 'Fresh Vegetables', category: 'Root Vegetables', subcategory: 'Onions & Garlic', items: [
    { name: 'Yellow Onion (3lb)', plu: '4093', unit: 'pkg',  base_price: 2.99 },  // power item
    { name: 'Red Onion',          plu: '4082', unit: 'each', base_price: 0.99 },
    { name: 'White Onion',        plu: '4665', unit: 'each', base_price: 0.89 },
    { name: 'Garlic (head)',      plu: '4608', unit: 'each', base_price: 0.79 },
    { name: 'Shallots',           plu: '4256', unit: 'lb',   base_price: 2.99 },
  ]},
  { department: 'Fresh Vegetables', category: 'Root Vegetables', subcategory: 'Carrots & Beets', items: [
    { name: 'Carrots (1lb bag)',  plu: '4562', unit: 'pkg',  base_price: 1.49 },
    { name: 'Rainbow Carrots',    plu: '3473', unit: 'bunch',base_price: 2.99 },
    { name: 'Red Beets (bunch)',  plu: '4350', unit: 'bunch',base_price: 2.99 },
    { name: 'Golden Beets',       plu: '3534', unit: 'bunch',base_price: 3.49 },
  ]},

  { department: 'Fresh Vegetables', category: 'Cruciferous', subcategory: 'Broccoli & Cauliflower', items: [
    { name: 'Broccoli Crown',     plu: '4548', unit: 'each', base_price: 2.49 },
    { name: 'Broccoli Florets (12oz)',plu:'3388',unit:'pkg', base_price: 3.99 },
    { name: 'Cauliflower Head',   plu: '4645', unit: 'each', base_price: 2.99 },
    { name: 'Broccolini (bunch)', plu: '3278', unit: 'bunch',base_price: 2.99 },
  ]},
  { department: 'Fresh Vegetables', category: 'Cruciferous', subcategory: 'Cabbage & Brussels Sprouts', items: [
    { name: 'Green Cabbage',      plu: '4085', unit: 'each', base_price: 1.99 },
    { name: 'Red Cabbage',        plu: '4086', unit: 'each', base_price: 2.29 },
    { name: 'Brussels Sprouts (lb)',plu:'4538',unit:'lb',    base_price: 2.99 },
  ]},

  { department: 'Fresh Vegetables', category: 'Stalks & Stems', subcategory: 'Asparagus & Celery', items: [
    { name: 'Asparagus (bunch)',  plu: '4080', unit: 'bunch',base_price: 3.49 },
    { name: 'Celery (bunch)',     plu: '4071', unit: 'each', base_price: 2.29 },
    { name: 'Fennel (bulb)',      plu: '4515', unit: 'each', base_price: 2.49 },
    { name: 'Rhubarb',            plu: '4503', unit: 'lb',   base_price: 3.99 },
  ]},

  { department: 'Fresh Vegetables', category: 'Vines & Pods', subcategory: 'Tomatoes', items: [
    { name: 'Beefsteak Tomato',   plu: '4664', unit: 'each', base_price: 1.29 },  // power item
    { name: 'Roma Tomato',        plu: '3421', unit: 'each', base_price: 0.79 },
    { name: 'Cherry Tomatoes (pint)',plu:'3068',unit:'pkg',  base_price: 3.99 },
    { name: 'Grape Tomatoes (pint)',plu:'3078',unit:'pkg',   base_price: 3.49 },
    { name: 'Heirloom Tomato',    plu: '3539', unit: 'lb',   base_price: 3.99 },
  ]},
  { department: 'Fresh Vegetables', category: 'Vines & Pods', subcategory: 'Cucumbers & Peppers', items: [
    { name: 'English Cucumber',   plu: '4592', unit: 'each', base_price: 1.99 },
    { name: 'Kirby Cucumber',     plu: '4062', unit: 'each', base_price: 0.79 },
    { name: 'Green Bell Pepper',  plu: '4065', unit: 'each', base_price: 0.99 },
    { name: 'Red Bell Pepper',    plu: '4688', unit: 'each', base_price: 1.49 },
    { name: 'Yellow Bell Pepper', plu: '4689', unit: 'each', base_price: 1.49 },
    { name: 'Jalapeño',           plu: '4068', unit: 'each', base_price: 0.39 },
    { name: 'Serrano Pepper',     plu: '4521', unit: 'each', base_price: 0.29 },
  ]},
  { department: 'Fresh Vegetables', category: 'Vines & Pods', subcategory: 'Squash & Zucchini', items: [
    { name: 'Zucchini',           plu: '4067', unit: 'each', base_price: 0.99 },
    { name: 'Yellow Squash',      plu: '4668', unit: 'each', base_price: 0.99 },
    { name: 'Butternut Squash',   plu: '4759', unit: 'each', base_price: 2.99 },
    { name: 'Acorn Squash',       plu: '4755', unit: 'each', base_price: 2.49 },
    { name: 'Spaghetti Squash',   plu: '4764', unit: 'each', base_price: 2.99 },
  ]},
  { department: 'Fresh Vegetables', category: 'Vines & Pods', subcategory: 'Beans & Peas', items: [
    { name: 'Green Beans (lb)',   plu: '4066', unit: 'lb',   base_price: 1.99 },
    { name: 'Sugar Snap Peas (lb)',plu:'3398', unit: 'lb',   base_price: 3.49 },
    { name: 'Snow Peas (lb)',     plu: '3399', unit: 'lb',   base_price: 3.49 },
  ]},

  // ══════════════════════════════════════════════
  // DEPARTMENT: Specialty & Value-Added
  // ══════════════════════════════════════════════

  { department: 'Specialty & Value-Added', category: 'Herbs', subcategory: 'Fresh Herbs', items: [
    { name: 'Cilantro (bunch)',   plu: '4889', unit: 'bunch',base_price: 0.99 },
    { name: 'Basil (bunch)',      plu: '4888', unit: 'bunch',base_price: 1.99 },
    { name: 'Flat-Leaf Parsley',  plu: '4899', unit: 'bunch',base_price: 0.99 },
    { name: 'Mint (bunch)',       plu: '4902', unit: 'bunch',base_price: 1.99 },
    { name: 'Rosemary (bunch)',   plu: '4905', unit: 'bunch',base_price: 2.49 },
    { name: 'Thyme (bunch)',      plu: '4906', unit: 'bunch',base_price: 2.49 },
    { name: 'Dill (bunch)',       plu: '4901', unit: 'bunch',base_price: 1.99 },
  ]},

  { department: 'Specialty & Value-Added', category: 'Mushrooms', subcategory: 'Fresh Mushrooms', items: [
    { name: 'White Button (8oz)',  plu: '4709', unit: 'pkg', base_price: 2.99 },
    { name: 'Cremini (8oz)',       plu: '4710', unit: 'pkg', base_price: 3.49 },
    { name: 'Portobello (2pk)',    plu: '4711', unit: 'pkg', base_price: 3.99 },
    { name: 'Oyster Mushroom',     plu: '3283', unit: 'lb',  base_price: 7.99 },
    { name: 'Shiitake (3.5oz)',    plu: '4712', unit: 'pkg', base_price: 4.49 },
  ]},

  { department: 'Specialty & Value-Added', category: 'Value-Added / Pre-cut', subcategory: 'Pre-cut Fruit', items: [
    { name: 'Fruit Bowl (32oz)',   plu: '3601', unit: 'pkg', base_price: 7.99 },
    { name: 'Sliced Melon (32oz)',  plu: '3602', unit: 'pkg', base_price: 5.99 },
    { name: 'Berry Mix (12oz)',    plu: '3603', unit: 'pkg', base_price: 5.49 },
    { name: 'Pineapple Chunks (20oz)',plu:'3604',unit:'pkg', base_price: 4.99 },
  ]},
  { department: 'Specialty & Value-Added', category: 'Value-Added / Pre-cut', subcategory: 'Pre-cut Vegetables', items: [
    { name: 'Stir-fry Mix (12oz)', plu: '3611', unit: 'pkg', base_price: 4.49 },
    { name: 'Zoodles (10oz)',      plu: '3612', unit: 'pkg', base_price: 3.99 },
    { name: 'Diced Butternut (20oz)',plu:'3613',unit:'pkg',  base_price: 4.49 },
    { name: 'Broccoli & Cauliflower Mix',plu:'3614',unit:'pkg',base_price:3.99},
  ]},

  { department: 'Specialty & Value-Added', category: 'Organic', subcategory: 'Organic Fruit', items: [
    { name: 'Organic Strawberries',plu: '94048', unit: 'pkg', base_price: 5.99 },
    { name: 'Organic Blueberries', plu: '94129', unit: 'pkg', base_price: 5.99 },
    { name: 'Organic Gala Apple',  plu: '94133', unit: 'each',base_price: 1.79 },
    { name: 'Organic Lemon',       plu: '94053', unit: 'each',base_price: 1.29 },
  ]},
  { department: 'Specialty & Value-Added', category: 'Organic', subcategory: 'Organic Vegetables', items: [
    { name: 'Organic Baby Spinach',plu: '94943', unit: 'pkg', base_price: 5.99 },
    { name: 'Organic Kale',        plu: '94467', unit: 'bunch',base_price:3.49},
    { name: 'Organic Carrots (lb)',plu: '94562', unit: 'pkg', base_price: 2.49 },
    { name: 'Organic Broccoli',    plu: '94548', unit: 'each',base_price: 3.49 },
  ]},

];

// ─── Power Items (high velocity, spread to pull shoppers through dept) ────────
const POWER_ITEM_PLUS = new Set(['4011','94011','4072','4093','4072','4664','4085']);

// ─── Shrinkage config by subcategory ─────────────────────────────────────────
// [min%, max%] of units ordered that are lost
const SHRINK_BY_SUBCAT = {
  'Strawberries & Blueberries':  [0.10, 0.22],
  'Raspberries & Blackberries':  [0.12, 0.25],
  'Peaches & Nectarines':        [0.06, 0.15],
  'Plums & Cherries':            [0.07, 0.16],
  'Pre-cut Fruit':               [0.08, 0.20],
  'Pre-cut Vegetables':          [0.06, 0.14],
  'Fresh Herbs':                 [0.08, 0.18],
  'Leafy Greens & Salads':       [0.05, 0.14],
  'Spinach & Kale':              [0.06, 0.16],
  'Pre-cut Salads':              [0.07, 0.15],
  'Tomatoes':                    [0.05, 0.12],
  'Fresh Mushrooms':             [0.07, 0.15],
  'Asparagus & Celery':          [0.04, 0.10],
  'Watermelon':                  [0.02, 0.06],
  'Potatoes':                    [0.01, 0.04],
  'Onions & Garlic':             [0.01, 0.03],
  'Apples':                      [0.02, 0.06],
  'default':                     [0.03, 0.09],
};

const SHRINK_REASONS = [
  'Overripe',
  'Damaged in Transit',
  'Mold / Pest',
  'Theft / Shrinkage',
  'Over-ordering',
  'Temperature Failure',
  'Cosmetic Defect',
];

// ─── Seasonal demand multipliers ─────────────────────────────────────────────
function seasonalMult(dateStr, subcategory) {
  const m = new Date(dateStr).getMonth(); // 0=Jan
  const summer  = new Set([4,5,6,7]);
  const fall    = new Set([8,9,10]);
  const winter  = new Set([11,0,1]);
  const spring  = new Set([2,3,4]);

  if (['Strawberries & Blueberries','Raspberries & Blackberries'].includes(subcategory))
    return summer.has(m) ? rand(1.5, 2.0) : rand(0.5, 0.8);
  if (['Peaches & Nectarines','Plums & Cherries'].includes(subcategory))
    return summer.has(m) ? rand(1.6, 2.2) : rand(0.2, 0.5);
  if (['Watermelon','Cantaloupe & Honeydew'].includes(subcategory))
    return summer.has(m) ? rand(1.8, 2.5) : rand(0.2, 0.4);
  if (['Squash & Zucchini','Cabbage & Brussels Sprouts'].includes(subcategory))
    return fall.has(m) ? rand(1.3, 1.6) : rand(0.8, 1.1);
  if (['Grapefruit & Tangerines','Oranges'].includes(subcategory))
    return winter.has(m) ? rand(1.4, 1.7) : rand(0.7, 1.0);
  if (['Asparagus & Celery'].includes(subcategory))
    return spring.has(m) ? rand(1.4, 1.7) : rand(0.7, 1.0);
  if (['Bananas','Potatoes','Onions & Garlic'].includes(subcategory))
    return rand(0.9, 1.1); // power items are year-round stable
  return rand(0.85, 1.15);
}

// Weekend uplift
function dayMult(dateStr) {
  const dow = new Date(dateStr).getDay();
  return (dow === 0 || dow === 6) ? rand(1.2, 1.5) : 1.0;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function rand(min, max) { return Math.random() * (max - min) + min; }
function randInt(min, max) { return Math.floor(rand(min, max + 1)); }
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function round2(n) { return Math.round(n * 100) / 100; }

function dateRange(startISO, endISO) {
  const dates = [];
  const cur = new Date(startISO);
  const end = new Date(endISO);
  while (cur <= end) {
    dates.push(cur.toISOString().split('T')[0]);
    cur.setDate(cur.getDate() + 1);
  }
  return dates;
}

// ─── Dataset builders ─────────────────────────────────────────────────────────

function buildSalesDocs(dates) {
  const docs = [];
  for (const date of dates) {
    for (const store of STORES) {
      for (const entry of TAXONOMY) {
        const { department, category, subcategory, items } = entry;
        for (const item of items) {
          const isPower  = POWER_ITEM_PLUS.has(item.plu);
          const seasonal = seasonalMult(date, subcategory);
          const dayM     = dayMult(date);
          const storeM   = store.storeM * rand(0.9, 1.1);  // persistent base × small daily jitter
          // Power items sell 3–8x more than regular items
          const baseUnits = isPower ? rand(40, 120) : rand(3, 30);
          const units     = Math.max(0, Math.round(baseUnits * seasonal * dayM * storeM));
          if (units === 0) continue;
          const price         = round2(item.base_price * rand(0.95, 1.05));
          const revenue       = round2(units * price);
          const transactions  = randInt(Math.ceil(units / 4), Math.max(1, units));
          docs.push({
            __instance_id: INSTANCE_ID,
            __ingested_at: new Date().toISOString(),
            date,
            store_id:      store.id,
            store_name:    store.name,
            city:          store.city,
            state:         store.state,
            region:        store.region,
            store_type:    store.store_type,
            department,
            category,
            subcategory,
            item_name:     item.name,
            plu_code:      item.plu,
            unit_type:     item.unit,
            units_sold:    units,
            unit_price:    price,
            revenue,
            transactions,
          });
        }
      }
    }
  }
  return docs;
}

function buildShrinkageDocs(dates) {
  const docs = [];
  for (const date of dates) {
    for (const store of STORES) {
      for (const entry of TAXONOMY) {
        const { department, category, subcategory, items } = entry;
        const [minR, maxR] = SHRINK_BY_SUBCAT[subcategory] || SHRINK_BY_SUBCAT['default'];
        for (const item of items) {
          // Not every item has shrink every day
          if (Math.random() < 0.45) continue;
          const shrinkRate = round2(rand(minR, maxR));
          const storeM     = store.storeM * rand(0.9, 1.1);  // larger stores carry more volume = more absolute shrink
          const baseUnits  = rand(5, 40);
          const unitsLost  = Math.max(1, Math.round(baseUnits * shrinkRate * storeM));
          const lossValue  = round2(unitsLost * item.base_price);
          docs.push({
            __instance_id: INSTANCE_ID,
            __ingested_at: new Date().toISOString(),
            date,
            store_id:      store.id,
            store_name:    store.name,
            city:          store.city,
            state:         store.state,
            region:        store.region,
            store_type:    store.store_type,
            department,
            category,
            subcategory,
            item_name:     item.name,
            plu_code:      item.plu,
            unit_type:     item.unit,
            shrink_reason: pick(SHRINK_REASONS),
            units_lost:    unitsLost,
            loss_value:    lossValue,
            shrink_rate:   shrinkRate,
          });
        }
      }
    }
  }
  return docs;
}

const CHANNELS = ['Walk-in', 'Farmers Market', 'CSA Subscription', 'Online Order', 'Referral'];

function buildCustomerDocs(dates) {
  const docs = [];
  for (const date of dates) {
    for (const store of STORES) {
      const dayM = dayMult(date);
      for (const type of ['New', 'Recurring']) {
        for (const channel of CHANNELS) {
          let prob = 0.65;
          if (type === 'Recurring' && channel === 'CSA Subscription') prob = 0.90;
          if (type === 'New'       && channel === 'Referral')         prob = 0.40;
          if (type === 'New'       && channel === 'Online Order')     prob = 0.50;
          if (Math.random() > prob) continue;
          const baseCount  = type === 'Recurring' ? rand(10, 50) : rand(2, 18);
          const count      = Math.max(1, Math.round(baseCount * dayM));
          const avgSpend   = type === 'Recurring' ? round2(rand(22, 65)) : round2(rand(12, 40));
          const totalSpend = round2(count * avgSpend);
          docs.push({
            __instance_id:       INSTANCE_ID,
            __ingested_at:       new Date().toISOString(),
            date,
            store_id:            store.id,
            store_name:          store.name,
            city:                store.city,
            state:               store.state,
            region:              store.region,
            store_type:          store.store_type,
            customer_type:       type,
            acquisition_channel: channel,
            customer_count:      count,
            total_spend:         totalSpend,
            avg_spend:           avgSpend,
            visit_count:         type === 'Recurring' ? randInt(1, 3) : 1,
          });
        }
      }
    }
  }
  return docs;
}

// ─── Schema definitions ───────────────────────────────────────────────────────

const SALES_FIELDS = [
  { name: 'date',         type: 'date',    role: 'segment' },
  { name: 'store_id',     type: 'keyword', role: 'segment' },
  { name: 'store_name',   type: 'keyword', role: 'segment' },
  { name: 'city',         type: 'keyword', role: 'segment' },
  { name: 'state',        type: 'keyword', role: 'segment' },
  { name: 'region',       type: 'keyword', role: 'segment' },
  { name: 'store_type',   type: 'keyword', role: 'segment' },
  { name: 'department',   type: 'keyword', role: 'segment' },
  { name: 'category',     type: 'keyword', role: 'segment' },
  { name: 'subcategory',  type: 'keyword', role: 'segment' },
  { name: 'item_name',    type: 'keyword', role: 'segment' },
  { name: 'plu_code',     type: 'keyword', role: 'segment' },
  { name: 'unit_type',    type: 'keyword', role: 'segment' },
  { name: 'units_sold',   type: 'integer', role: 'metric', aggregationType: 'sum'   },
  { name: 'unit_price',   type: 'float',   role: 'metric', aggregationType: 'avg'   },
  { name: 'revenue',      type: 'float',   role: 'metric', aggregationType: 'sum'   },
  { name: 'transactions', type: 'integer', role: 'metric', aggregationType: 'sum'   },
];

const SHRINKAGE_FIELDS = [
  { name: 'date',          type: 'date',    role: 'segment' },
  { name: 'store_id',      type: 'keyword', role: 'segment' },
  { name: 'store_name',    type: 'keyword', role: 'segment' },
  { name: 'city',          type: 'keyword', role: 'segment' },
  { name: 'state',         type: 'keyword', role: 'segment' },
  { name: 'region',        type: 'keyword', role: 'segment' },
  { name: 'store_type',    type: 'keyword', role: 'segment' },
  { name: 'department',    type: 'keyword', role: 'segment' },
  { name: 'category',      type: 'keyword', role: 'segment' },
  { name: 'subcategory',   type: 'keyword', role: 'segment' },
  { name: 'item_name',     type: 'keyword', role: 'segment' },
  { name: 'plu_code',      type: 'keyword', role: 'segment' },
  { name: 'unit_type',     type: 'keyword', role: 'segment' },
  { name: 'shrink_reason', type: 'keyword', role: 'segment' },
  { name: 'units_lost',    type: 'integer', role: 'metric'  },
  { name: 'loss_value',    type: 'float',   role: 'metric'  },
  { name: 'shrink_rate',   type: 'float',   role: 'metric'  },
];

const CUSTOMERS_FIELDS = [
  { name: 'date',                type: 'date',    role: 'segment' },
  { name: 'store_id',            type: 'keyword', role: 'segment' },
  { name: 'store_name',          type: 'keyword', role: 'segment' },
  { name: 'city',                type: 'keyword', role: 'segment' },
  { name: 'state',               type: 'keyword', role: 'segment' },
  { name: 'region',              type: 'keyword', role: 'segment' },
  { name: 'store_type',          type: 'keyword', role: 'segment' },
  { name: 'customer_type',       type: 'keyword', role: 'segment' },
  { name: 'acquisition_channel', type: 'keyword', role: 'segment' },
  { name: 'customer_count',      type: 'integer', role: 'metric'  },
  { name: 'total_spend',         type: 'float',   role: 'metric'  },
  { name: 'avg_spend',           type: 'float',   role: 'metric'  },
  { name: 'visit_count',         type: 'integer', role: 'metric'  },
];

// ─── ES helpers ───────────────────────────────────────────────────────────────

function toEsMapping(fields) {
  const props = {
    __instance_id: { type: 'keyword' },
    __ingested_at: { type: 'date' },
  };
  fields.forEach(f => {
    props[f.name] = {
      type: f.type === 'date' ? 'date'
          : f.type === 'float' ? 'double'
          : f.type === 'integer' ? 'integer'
          : 'keyword'
    };
  });
  return { properties: props };
}

async function createIndex(name, mappings) {
  const indexName = `${INSTANCE_ID}__${name}__v1`;
  const alias     = `${INSTANCE_ID}__${name}`;
  if (await es.indices.exists({ index: indexName })) {
    console.log(`  Dropping existing ${indexName}...`);
    await es.indices.delete({ index: indexName });
  }
  await es.indices.create({
    index: indexName,
    body: { mappings, settings: { number_of_replicas: 0, refresh_interval: '30s' } }
  });
  try { await es.indices.deleteAlias({ index: '_all', name: alias }); } catch (_) {}
  await es.indices.putAlias({ index: indexName, name: alias });
  console.log(`  Created ${indexName} → alias ${alias}`);
}

async function bulkIndex(alias, docs) {
  const CHUNK = 1000;
  let total = 0;
  for (let i = 0; i < docs.length; i += CHUNK) {
    const chunk = docs.slice(i, i + CHUNK);
    const body  = chunk.flatMap(d => [{ index: { _index: alias } }, d]);
    const { errors, items } = await es.bulk({ body, refresh: false });
    if (errors) {
      const errs = items.filter(it => it.index?.error);
      if (errs.length) console.warn(`  ${errs.length} bulk errors`, errs[0]?.index?.error?.reason);
    }
    total += chunk.length;
    if (total % 10000 === 0 || total === docs.length) {
      process.stdout.write(`\r  Indexed ${total.toLocaleString()}/${docs.length.toLocaleString()}`);
    }
  }
  await es.indices.refresh({ index: alias });
  console.log(`\r  Indexed ${total.toLocaleString()} docs into ${alias}  ✓`);
}

// ─── DB registration ──────────────────────────────────────────────────────────

async function registerDataset(name, fields) {
  if (!pool) { console.log('  No DATABASE_URL — skipping DB registration'); return; }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO clients (client_id, name) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [INSTANCE_ID, 'Produce Stand']
    );
    const label    = name.replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase());
    const alias    = `${INSTANCE_ID}__${name}`;
    const esIndex  = `${INSTANCE_ID}__${name}__v1`;
    const { rows: [dd] } = await client.query(`
      INSERT INTO dataset_definitions (client_id, name, label, current_version, es_alias, is_active)
      VALUES ($1,$2,$3,1,$4,TRUE)
      ON CONFLICT (client_id, name) DO UPDATE
        SET label=$3, current_version=1, es_alias=$4, is_active=TRUE, updated_at=NOW()
      RETURNING id
    `, [INSTANCE_ID, name, label, alias]);
    const fieldDefs = fields.map(f => ({
      name:            f.name,
      label:           f.name.replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase()),
      fieldType:       f.role,
      segmentType:     f.type,
      aggregationType: f.role === 'metric' ? (f.aggregationType || 'sum').toUpperCase() : null,
      isFilterable:    true,
      isGroupable:     f.role === 'segment',
    }));
    await client.query(`
      INSERT INTO dataset_schema_versions (dataset_id, version, fields, es_index, compat_status)
      VALUES ($1,1,$2,$3,'compatible')
      ON CONFLICT (dataset_id, version) DO UPDATE SET fields=$2, es_index=$3
    `, [dd.id, JSON.stringify(fieldDefs), esIndex]);
    await client.query('COMMIT');
    console.log(`  Registered ${name} in DB ✓`);
    return dd.id;  // return dataset_id for lookup wiring
  } catch (e) {
    await client.query('ROLLBACK');
    console.warn(`  DB registration failed for ${name}:`, e.message);
    return null;
  } finally { client.release(); }
}

// ─── Lookup dataset registration ────────────────────────────────────────────
// Registers the Stores lookup dataset and seeds one row per store.
// Also wires store_id → stores lookup on sales, shrinkage, and customers.
// auto_populated=false on seed rows so Designer edits won't be overwritten by ingest.

const STORES_LOOKUP_FIELDS = [
  { field_name: 'store_id',    label: 'Store ID',      field_type: 'text',   is_key_field: true,  is_display_field: false, is_required: true,  sort_order: 0 },
  { field_name: 'store_name',  label: 'Store Name',    field_type: 'text',   is_key_field: false, is_display_field: true,  is_required: true,  sort_order: 1 },
  { field_name: 'city',        label: 'City',          field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 2 },
  { field_name: 'state',       label: 'State',         field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 3 },
  { field_name: 'region',      label: 'Region',        field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 4 },
  { field_name: 'store_type',  label: 'Store Type',    field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 5 },
  // Enrichment stubs — empty at seed time, populated via Designer UI later
  { field_name: 'address',     label: 'Address',       field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 6 },
  { field_name: 'manager',     label: 'Manager',       field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 7 },
  { field_name: 'sq_footage',  label: 'Sq Footage',    field_type: 'number', is_key_field: false, is_display_field: false, is_required: false, sort_order: 8 },
  { field_name: 'open_date',   label: 'Open Date',     field_type: 'date',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 9 },
  { field_name: 'phone',       label: 'Phone',         field_type: 'text',   is_key_field: false, is_display_field: false, is_required: false, sort_order: 10 },
];

// Datasets that carry store_id and should get the lookup relationship wired
const STORE_LINKED_DATASETS = ['sales', 'shrinkage', 'customers'];

async function registerLookupDatasets(datasetIdMap) {
  if (!pool) { console.log('  No DATABASE_URL — skipping lookup registration'); return; }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Upsert lookup_datasets row
    const { rows: [ld] } = await client.query(`
      INSERT INTO lookup_datasets (client_id, name, label, description, key_field, icon)
      VALUES ($1, 'stores', 'Stores', 'Master record for each store location', 'store_id', 'store')
      ON CONFLICT (client_id, name)
      DO UPDATE SET label='Stores', key_field='store_id', updated_at=NOW()
      RETURNING id
    `, [INSTANCE_ID]);
    const lookupId = ld.id;

    // 2. Upsert lookup_dataset_fields
    for (const f of STORES_LOOKUP_FIELDS) {
      await client.query(`
        INSERT INTO lookup_dataset_fields
          (lookup_dataset_id, field_name, label, field_type, is_key_field, is_display_field, is_required, sort_order)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (lookup_dataset_id, field_name)
        DO UPDATE SET label=$3, field_type=$4, is_key_field=$5, is_display_field=$6, sort_order=$8
      `, [lookupId, f.field_name, f.label, f.field_type, f.is_key_field, f.is_display_field, f.is_required, f.sort_order]);
    }

    // 3. Upsert lookup_dataset_rows — one per store
    // auto_populated=false: these are seed records, Designer edits take precedence
    for (const store of STORES) {
      const data = {
        store_name:  store.name,
        city:        store.city,
        state:       store.state,
        region:      store.region,
        store_type:  store.store_type,
        // Enrichment fields — empty stubs ready for Designer UI
        address:     null,
        manager:     null,
        sq_footage:  null,
        open_date:   null,
        phone:       null,
      };
      await client.query(`
        INSERT INTO lookup_dataset_rows (lookup_dataset_id, key_value, data, auto_populated)
        VALUES ($1,$2,$3,FALSE)
        ON CONFLICT (lookup_dataset_id, key_value)
        DO UPDATE SET data = lookup_dataset_rows.data || $3::jsonb, updated_at=NOW()
      `, [lookupId, store.id, JSON.stringify(data)]);
    }
    console.log(`  Seeded ${STORES.length} store rows into lookup_dataset_rows ✓`);

    // 4. Wire store_id → stores lookup on each linked dataset via dataset_field_metadata
    for (const dsName of STORE_LINKED_DATASETS) {
      const dsId = datasetIdMap[dsName];
      if (!dsId) { console.warn(`  dataset_id not found for '${dsName}' — skipping relationship wire`); continue; }
      await client.query(`
        INSERT INTO dataset_field_metadata
          (dataset_id, field_name, label, field_type, lookup_dataset_id, lookup_key_field, sort_order, updated_by)
        VALUES ($1,'store_id','Store ID','segment',$2,'store_id',0,'seed')
        ON CONFLICT (dataset_id, field_name)
        DO UPDATE SET lookup_dataset_id=$2, lookup_key_field='store_id', updated_at=NOW()
      `, [dsId, lookupId]);
    }
    console.log(`  Wired store_id → stores lookup on [${STORE_LINKED_DATASETS.join(', ')}] ✓`);

    await client.query('COMMIT');
    console.log(`  Lookup datasets registered ✓`);
  } catch (e) {
    await client.query('ROLLBACK');
    console.warn('  Lookup registration failed:', e.message);
    throw e;
  } finally { client.release(); }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const dates = dateRange('2024-01-01', '2026-12-31');
  const itemCount = TAXONOMY.reduce((s, e) => s + e.items.length, 0);
  console.log(`DDNL Produce Seed`);
  console.log(`Hierarchy: ${[...new Set(TAXONOMY.map(e=>e.department))].length} departments, ` +
              `${[...new Set(TAXONOMY.map(e=>e.category))].length} categories, ` +
              `${[...new Set(TAXONOMY.map(e=>e.subcategory))].length} subcategories, ` +
              `${itemCount} items (with PLU codes)`);
  console.log(`${dates.length} days × ${STORES.length} stores`);
  console.log(`ES: ${ES_URL}  |  Instance: ${INSTANCE_ID}\n`);

  // Sales
  console.log('── Sales ──────────────────────────────────────────────────────────────');
  await createIndex('sales', toEsMapping(SALES_FIELDS));
  process.stdout.write('  Building docs...');
  const salesDocs = buildSalesDocs(dates);
  console.log(`\r  Generated ${salesDocs.length.toLocaleString()} sales docs`);
  await bulkIndex(`${INSTANCE_ID}__sales`, salesDocs);
  const salesId = await registerDataset('sales', SALES_FIELDS);

  // Shrinkage
  console.log('\n── Shrinkage ──────────────────────────────────────────────────────────');
  await createIndex('shrinkage', toEsMapping(SHRINKAGE_FIELDS));
  process.stdout.write('  Building docs...');
  const shrinkDocs = buildShrinkageDocs(dates);
  console.log(`\r  Generated ${shrinkDocs.length.toLocaleString()} shrinkage docs`);
  await bulkIndex(`${INSTANCE_ID}__shrinkage`, shrinkDocs);
  const shrinkId = await registerDataset('shrinkage', SHRINKAGE_FIELDS);

  // Customers
  console.log('\n── Customers ──────────────────────────────────────────────────────────');
  await createIndex('customers', toEsMapping(CUSTOMERS_FIELDS));
  process.stdout.write('  Building docs...');
  const custDocs = buildCustomerDocs(dates);
  console.log(`\r  Generated ${custDocs.length.toLocaleString()} customer docs`);
  await bulkIndex(`${INSTANCE_ID}__customers`, custDocs);
  const custId = await registerDataset('customers', CUSTOMERS_FIELDS);

  // Lookup datasets — Stores master record
  console.log('\n── Lookup Datasets ───────────────────────────────────────────────────');
  await registerLookupDatasets({ sales: salesId, shrinkage: shrinkId, customers: custId });

  console.log('\n✓ Seed complete');
  if (pool) await pool.end();
  process.exit(0);
})().catch(e => { console.error('\nSeed failed:', e.message); process.exit(1); });
