"""
=============================================================================
ISell Store - Complete Telegram Digital Selling Store Bot
Built with Python 3.12+, aiogram 3.x, and MySQL (aiomysql)
Compatible with Local Polling and Render Free Deployment

ALL APPLICATION LOGIC IS STRICTLY CONTAINED INSIDE THIS SINGLE FILE.
=============================================================================
"""

import os
import sys
import math
import html
import asyncio
import logging
from urllib.parse import urlparse
from decimal import Decimal
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from dotenv import load_dotenv

import aiomysql
from aiohttp import web

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    BotCommand
)
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramBadRequest

# =============================================================================
# 1. CONFIGURATION & LOGGING
# =============================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ISellStore")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    logger.error("CRITICAL: BOT_TOKEN is not defined in environment variables or .env!")
    sys.exit(1)

# Database credentials (supports single URI or discrete variables)
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MYSQL_URL")
if DATABASE_URL:
    parsed_db = urlparse(DATABASE_URL)
    MYSQL_HOST = parsed_db.hostname or "localhost"
    MYSQL_PORT = parsed_db.port or 3306
    MYSQL_USER = parsed_db.username or "root"
    MYSQL_PASSWORD = parsed_db.password or ""
    MYSQL_DATABASE = parsed_db.path.lstrip("/") or "isell_store"
else:
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "isell_store")

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "7695407294"))
except ValueError:
    ADMIN_ID = 7695407294

DEFAULT_BINANCE_ID = os.getenv("BINANCE_ID", "898551245")
DEFAULT_USDT_ADDR = os.getenv("USDT_BEP20_ADDRESS", "0xa271f85cc7340aceec7d3f7711feae8925bd50fb")
DEFAULT_SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "ezrani").replace("@", "")
DEFAULT_STORE_NAME = os.getenv("STORE_NAME", "ISell Store")
DEFAULT_CURRENCY = os.getenv("STORE_CURRENCY", "USDT")
RENDER_PORT = int(os.getenv("PORT", "10000"))

db_pool: Optional[aiomysql.Pool] = None

# Initial catalog seeds (No stock is seeded)
CATALOG_SEEDS = [
    ("ChatGPT", "🤖"), ("Canva", "🎨"), ("Spotify", "🎵"), ("Gamma AI", "📊"),
    ("Netflix", "🎬"), ("Duolingo", "🦉"), ("YouTube", "📺"), ("Microsoft 365", "💻"),
    ("Turnitin", "📝"), ("Adobe", "📐"), ("Claude", "🟣"), ("Grok", "⚡"),
    ("Elsa Speaks", "🗣"), ("Figma", "🖌"), ("Veo 4", "🎥"), ("CapCut", "✂️"),
    ("Kling", "🎞"), ("Cursor", "🖱"), ("Zoom", "📹"), ("Scribd", "📚"),
    ("HeyGen", "👤"), ("ElevenLabs", "🎙"), ("Wink", "✨"), ("Meitu", "💄"),
    ("TradingView", "📈"), ("HMA VPN", "🛡"), ("Discord Nitro", "🚀"), ("ExpressVPN", "🔒"),
    ("Windows Key", "🪟"), ("Freepik", "🖼"), ("iCloud", "☁️"), ("Quizizz", "🎯"),
    ("Perplexity", "🧠"), ("Wordwall", "🧩"), ("Notion", "📓"), ("Suno", "🎼"),
    ("Gemini", "♊"), ("Higgsfield", "🌌"), ("Kiro", "🤖"), ("Dreamina", "💭"),
    ("OpenArt", "🎨"), ("Quizlet", "📖"), ("Coursera", "🎓"), ("Kimi", "💡"),
    ("ArtCraft", "🎭")
]

# =============================================================================
# 2. DATABASE INITIALIZATION & MIGRATIONS
# =============================================================================

async def init_db():
    """Connect to MySQL, create tables with migrations, and seed initial data."""
    global db_pool
    logger.info("Connecting to MySQL at %s:%s (DB: %s)...", MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE)

    # 1. Attempt to ensure database exists (handled gracefully if cloud user lacks global CREATE DB privilege)
    try:
        conn = await aiomysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            autocommit=True
        )
        async with conn.cursor() as cur:
            await cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
        conn.close()
    except Exception as e:
        logger.warning("Notice on CREATE DATABASE: %s (Continuing connection)", e)

    # 2. Establish connection pool
    db_pool = await aiomysql.create_pool(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DATABASE,
        autocommit=True,
        minsize=2,
        maxsize=15,
        charset="utf8mb4"
    )

    # 3. Create tables & execute schema adjustments
    async with db_pool.acquire() as c:
        async with c.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    `key` VARCHAR(64) PRIMARY KEY,
                    `value` TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(64) NULL,
                    first_name VARCHAR(128) NULL,
                    last_name VARCHAR(128) NULL,
                    wallet_balance DECIMAL(10, 2) DEFAULT 0.00,
                    is_admin TINYINT(1) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Ensure wallet_balance column exists if upgrading an existing DB
            try:
                await cur.execute("ALTER TABLE users ADD COLUMN wallet_balance DECIMAL(10, 2) DEFAULT 0.00 AFTER last_name;")
            except Exception:
                pass

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL,
                    emoji VARCHAR(16) DEFAULT '📦',
                    description TEXT NULL,
                    sort_order INT DEFAULT 0,
                    is_active TINYINT(1) DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_products_active (is_active, sort_order)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    details TEXT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    warranty VARCHAR(128) DEFAULT '30 Days',
                    is_active TINYINT(1) DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                    INDEX idx_plans_product (product_id, is_active)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Migrations for existing plans table (warranty & details)
            try:
                await cur.execute("ALTER TABLE plans ADD COLUMN details TEXT NULL AFTER name;")
            except Exception:
                pass
            try:
                await cur.execute("ALTER TABLE plans ADD COLUMN warranty VARCHAR(128) DEFAULT '30 Days' AFTER price;")
            except Exception:
                pass

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    plan_id INT NOT NULL,
                    item_type ENUM('CODE', 'ACCOUNT', 'LICENSE', 'MANUAL') DEFAULT 'CODE',
                    content TEXT NOT NULL,
                    status ENUM('AVAILABLE', 'RESERVED', 'SOLD', 'DISABLED') DEFAULT 'AVAILABLE',
                    reserved_by_order_id BIGINT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (plan_id) REFERENCES plans(id) ON DELETE CASCADE,
                    INDEX idx_inventory_status (plan_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    plan_id INT NOT NULL,
                    product_id INT NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    currency VARCHAR(16) DEFAULT 'USDT',
                    status ENUM('PENDING_PAYMENT', 'PAYMENT_SUBMITTED', 'PAYMENT_REJECTED', 'PAID', 'PROCESSING', 'READY', 'DELIVERED', 'CANCELLED', 'REFUNDED') DEFAULT 'PENDING_PAYMENT',
                    payment_method VARCHAR(32) NULL,
                    tx_id VARCHAR(255) NULL,
                    proof_file_id VARCHAR(255) NULL,
                    delivery_data TEXT NULL,
                    fulfilled_by BIGINT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (plan_id) REFERENCES plans(id),
                    FOREIGN KEY (product_id) REFERENCES products(id),
                    INDEX idx_orders_user (user_id),
                    INDEX idx_orders_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS deposits (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    currency VARCHAR(16) DEFAULT 'USDT',
                    payment_method VARCHAR(32) NOT NULL,
                    tx_id VARCHAR(255) NULL,
                    proof_file_id VARCHAR(255) NULL,
                    status ENUM('PENDING', 'APPROVED', 'REJECTED') DEFAULT 'PENDING',
                    reviewed_by BIGINT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    INDEX idx_deposits_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_notifications (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    plan_id INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_user_plan (user_id, plan_id),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (plan_id) REFERENCES plans(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            await cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    action VARCHAR(64) NOT NULL,
                    details TEXT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_admin_logs (admin_id, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Default settings
            default_settings = {
                "store_name": DEFAULT_STORE_NAME,
                "binance_id": DEFAULT_BINANCE_ID,
                "usdt_bep20": DEFAULT_USDT_ADDR,
                "support_username": DEFAULT_SUPPORT_USERNAME,
                "currency": DEFAULT_CURRENCY
            }
            for k, v in default_settings.items():
                await cur.execute(
                    "INSERT IGNORE INTO settings (`key`, `value`) VALUES (%s, %s)", (k, v)
                )

            # Ensure admin record exists
            await cur.execute(
                "INSERT INTO users (id, username, first_name, is_admin) "
                "VALUES (%s, %s, %s, 1) "
                "ON DUPLICATE KEY UPDATE is_admin = 1",
                (ADMIN_ID, "ezrani", "Administrator")
            )

            # Seed catalog if empty
            for prod_name, emoji in CATALOG_SEEDS:
                await cur.execute("SELECT id FROM products WHERE name = %s", (prod_name,))
                existing = await cur.fetchone()
                if not existing:
                    await cur.execute(
                        "INSERT INTO products (name, emoji, description, is_active) VALUES (%s, %s, %s, 1)",
                        (prod_name, emoji, f"Official premium {prod_name} subscription & credentials.")
                    )

    logger.info(" Database connected")
    logger.info(" Tables ready & migrations verified")
    logger.info(" Admin loaded")
    logger.info(" Product catalog loaded")
    logger.info("🤖 ISell Store online")


# =============================================================================
# 3. DATABASE QUERIES & CORE LOGIC
# =============================================================================

async def get_setting(key: str, default: str = "") -> str:
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT `value` FROM settings WHERE `key` = %s", (key,))
            row = await cur.fetchone()
            return row["value"] if row else default

async def set_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO settings (`key`, `value`) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE `value` = %s",
                (key, value, value)
            )

async def log_admin_action(admin_id: int, action: str, details: str = ""):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO admin_logs (admin_id, action, details) VALUES (%s, %s, %s)",
                (admin_id, action, details)
            )

async def register_or_update_user(user: types.User):
    is_adm = 1 if user.id == ADMIN_ID else 0
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO users (id, username, first_name, last_name, is_admin) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE username=%s, first_name=%s, last_name=%s, "
                "is_admin = IF(id = %s, 1, is_admin)",
                (
                    user.id, user.username, user.first_name, user.last_name, is_adm,
                    user.username, user.first_name, user.last_name, ADMIN_ID
                )
            )

async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return await cur.fetchone()

async def get_products_with_stock_info(active_only: bool = True) -> List[Dict[str, Any]]:
    query = """
        SELECT 
            p.id, p.name, p.emoji, p.is_active, p.sort_order,
            COALESCE(SUM(CASE WHEN i.status = 'AVAILABLE' AND pl.is_active = 1 THEN 1 ELSE 0 END), 0) AS available_stock_count
        FROM products p
        LEFT JOIN plans pl ON pl.product_id = p.id AND pl.is_active = 1
        LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
        {where_clause}
        GROUP BY p.id
        ORDER BY p.sort_order ASC, p.name ASC;
    """
    where_clause = "WHERE p.is_active = 1" if active_only else ""
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query.format(where_clause=where_clause))
            return await cur.fetchall()

async def get_product_by_id(product_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM products WHERE id = %s", (product_id,))
            return await cur.fetchone()

async def get_plans_by_product_id(product_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
    query = """
        SELECT 
            pl.id, pl.product_id, pl.name, pl.details, pl.price, pl.warranty, pl.is_active,
            COUNT(CASE WHEN i.status = 'AVAILABLE' THEN 1 END) AS available_count
        FROM plans pl
        LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
        WHERE pl.product_id = %s {extra_where}
        GROUP BY pl.id
        ORDER BY pl.price ASC;
    """
    extra_where = "AND pl.is_active = 1" if active_only else ""
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query.format(extra_where=extra_where), (product_id,))
            return await cur.fetchall()

async def get_plan_by_id(plan_id: int) -> Optional[Dict[str, Any]]:
    query = """
        SELECT 
            pl.*, p.name AS product_name, p.emoji AS product_emoji,
            COUNT(CASE WHEN i.status = 'AVAILABLE' THEN 1 END) AS available_count
        FROM plans pl
        JOIN products p ON p.id = pl.product_id
        LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
        WHERE pl.id = %s
        GROUP BY pl.id;
    """
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (plan_id,))
            return await cur.fetchone()

async def create_order(user_id: int, plan_id: int) -> Optional[int]:
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT product_id, price FROM plans WHERE id = %s AND is_active = 1", (plan_id,))
            plan = await cur.fetchone()
            if not plan:
                return None
            
            await cur.execute(
                "INSERT INTO orders (user_id, plan_id, product_id, amount, status) "
                "VALUES (%s, %s, %s, %s, 'PENDING_PAYMENT')",
                (user_id, plan_id, plan["product_id"], plan["price"])
            )
            return cur.lastrowid

async def get_order_by_id(order_id: int) -> Optional[Dict[str, Any]]:
    query = """
        SELECT 
            o.*, 
            p.name AS product_name, p.emoji AS product_emoji,
            pl.name AS plan_name, pl.warranty, pl.details AS plan_details,
            u.username, u.first_name, u.last_name, u.wallet_balance
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN plans pl ON pl.id = o.plan_id
        JOIN users u ON u.id = o.user_id
        WHERE o.id = %s;
    """
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, (order_id,))
            return await cur.fetchone()

async def add_stock_items(plan_id: int, items: List[str], item_type: str = "CODE") -> int:
    inserted = 0
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            for item in items:
                cleaned = item.strip()
                if cleaned:
                    await cur.execute(
                        "INSERT INTO inventory (plan_id, item_type, content, status) VALUES (%s, %s, %s, 'AVAILABLE')",
                        (plan_id, item_type, cleaned)
                    )
                    inserted += 1
    return inserted

async def subscribe_stock_notification(user_id: int, plan_id: int) -> bool:
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    "INSERT IGNORE INTO stock_notifications (user_id, plan_id) VALUES (%s, %s)",
                    (user_id, plan_id)
                )
                return cur.rowcount > 0
            except Exception:
                return False

async def get_subscribers_for_plan(plan_id: int) -> List[int]:
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT user_id FROM stock_notifications WHERE plan_id = %s", (plan_id,))
            rows = await cur.fetchall()
            return [r["user_id"] for r in rows]

async def clear_subscribers_for_plan(plan_id: int):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM stock_notifications WHERE plan_id = %s", (plan_id,))

async def process_wallet_purchase(order_id: int, user_id: int) -> Tuple[bool, str]:
    """Deducts user wallet balance atomically and fulfills the order if stock exists."""
    async with db_pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # 1. Fetch user & verify balance
                await cur.execute("SELECT wallet_balance FROM users WHERE id = %s FOR UPDATE", (user_id,))
                user = await cur.fetchone()
                if not user:
                    await conn.rollback()
                    return False, "User not found."

                # 2. Fetch order
                await cur.execute("SELECT * FROM orders WHERE id = %s FOR UPDATE", (order_id,))
                order = await cur.fetchone()
                if not order or order["user_id"] != user_id:
                    await conn.rollback()
                    return False, "Order invalid."

                if order["status"] == "DELIVERED":
                    await conn.rollback()
                    return False, "Order already fulfilled."

                order_amount = Decimal(str(order["amount"]))
                user_balance = Decimal(str(user["wallet_balance"]))

                if user_balance < order_amount:
                    await conn.rollback()
                    return False, f"Insufficient wallet balance! Needed: ${order_amount:.2f}, Available: ${user_balance:.2f}"

                # 3. Check and claim inventory item
                await cur.execute(
                    "SELECT id, content, item_type FROM inventory "
                    "WHERE plan_id = %s AND status = 'AVAILABLE' "
                    "ORDER BY id ASC LIMIT 1 FOR UPDATE",
                    (order["plan_id"],)
                )
                item = await cur.fetchone()
                if not item:
                    await conn.rollback()
                    return False, "Sorry! This item just went out of stock. Your balance was not charged."

                # Deduct balance
                new_balance = user_balance - order_amount
                await cur.execute("UPDATE users SET wallet_balance = %s WHERE id = %s", (new_balance, user_id))

                # Mark inventory sold
                await cur.execute("UPDATE inventory SET status = 'SOLD', reserved_by_order_id = %s WHERE id = %s", (order_id, item["id"]))

                # Mark order delivered
                delivery_content = f"[{item['item_type']}]\n{item['content']}"
                await cur.execute(
                    "UPDATE orders SET status = 'DELIVERED', payment_method = 'WALLET', delivery_data = %s WHERE id = %s",
                    (delivery_content, order_id)
                )

            await conn.commit()
            return True, delivery_content
        except Exception as e:
            await conn.rollback()
            logger.error("Error processing wallet checkout: %s", e)
            return False, f"Transaction error: {str(e)}"

async def fulfill_single_order(order_id: int, admin_id: int, manual_delivery_text: Optional[str] = None) -> Tuple[bool, str]:
    """Atomically fulfills order from stock or manual input."""
    async with db_pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM orders WHERE id = %s FOR UPDATE", (order_id,))
                order = await cur.fetchone()
                if not order:
                    await conn.rollback()
                    return False, "Order not found."
                
                if order["status"] == "DELIVERED":
                    await conn.rollback()
                    return False, "Order has already been fulfilled."

                delivery_content = ""
                if manual_delivery_text and manual_delivery_text.strip():
                    delivery_content = manual_delivery_text.strip()
                else:
                    await cur.execute(
                        "SELECT id, content, item_type FROM inventory "
                        "WHERE plan_id = %s AND status = 'AVAILABLE' "
                        "ORDER BY id ASC LIMIT 1 FOR UPDATE",
                        (order["plan_id"],)
                    )
                    inv_item = await cur.fetchone()
                    if inv_item:
                        delivery_content = f"[{inv_item['item_type']}]\n{inv_item['content']}"
                        await cur.execute(
                            "UPDATE inventory SET status = 'SOLD', reserved_by_order_id = %s WHERE id = %s",
                            (order["id"], inv_item["id"])
                        )
                    else:
                        await conn.rollback()
                        return False, "No automated inventory in stock! Please add stock or input manual fulfillment."

                await cur.execute(
                    "UPDATE orders SET status = 'DELIVERED', delivery_data = %s, fulfilled_by = %s WHERE id = %s",
                    (delivery_content, admin_id, order_id)
                )

            await conn.commit()
            return True, delivery_content
        except Exception as e:
            await conn.rollback()
            logger.error("Error during order fulfillment: %s", e)
            return False, f"Fulfillment error: {str(e)}"


# =============================================================================
# 4. FSM STATE GROUPS
# =============================================================================

class CustomerPurchaseFSM(StatesGroup):
    waiting_for_payment_proof = State()

class CustomerDepositFSM(StatesGroup):
    waiting_for_amount = State()
    waiting_for_proof = State()

class CustomerSearchFSM(StatesGroup):
    waiting_for_query = State()

class AdminProductFSM(StatesGroup):
    waiting_for_name = State()
    waiting_for_emoji = State()
    waiting_for_description = State()

class AdminPlanFSM(StatesGroup):
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_details = State()
    waiting_for_warranty = State()

class AdminStockFSM(StatesGroup):
    waiting_for_items = State()

class AdminFulfillFSM(StatesGroup):
    waiting_for_text = State()

class AdminBroadcastFSM(StatesGroup):
    waiting_for_content = State()
    confirm_send = State()

class AdminSettingsFSM(StatesGroup):
    waiting_for_value = State()


# =============================================================================
# 5. BOT INITIALIZATION & UI BUILDERS
# =============================================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

# --- Customer Keyboards ---

def get_customer_home_kb(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🛍 Store", callback_data="cust_store_p0"),
         InlineKeyboardButton(text="💳 Wallet", callback_data="cust_wallet")],
        [InlineKeyboardButton(text="📦 My Orders", callback_data="cust_my_orders"),
         InlineKeyboardButton(text="🔎 Search", callback_data="cust_search")],
        [InlineKeyboardButton(text="👤 Profile", callback_data="cust_profile"),
         InlineKeyboardButton(text="🔔 Notifications", callback_data="cust_notifs")],
        [InlineKeyboardButton(text="📞 Support", callback_data="cust_support"),
         InlineKeyboardButton(text="ℹ️ Help", callback_data="cust_help")]
    ]
    if is_admin(user_id):
        buttons.append([InlineKeyboardButton(text="⚙️ ADMIN PANEL", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_products_grid_kb(products: List[Dict[str, Any]], page: int = 0, per_page: int = 9) -> InlineKeyboardMarkup:
    total_pages = max(1, math.ceil(len(products) / per_page))
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_products = products[start_idx:start_idx + per_page]

    keyboard: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []

    for idx, p in enumerate(page_products):
        stock_symbol = "🟢" if p["available_stock_count"] > 0 else "❌"
        emoji = p.get("emoji") or "📦"
        btn_text = f"{stock_symbol} {emoji} {p['name']}"
        row.append(InlineKeyboardButton(text=btn_text, callback_data=f"prod_view_{p['id']}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # Navigation & Refresh
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"cust_store_p{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"Page {page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"cust_store_p{page + 1}"))
    keyboard.append(nav_buttons)

    keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data=f"cust_store_p{page}"),
        InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_plans_kb(product_id: int, plans: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard: List[List[InlineKeyboardButton]] = []
    for pl in plans:
        stock = pl["available_count"]
        warranty_badge = f" • 🛡 {pl.get('warranty') or '30D'}"
        if stock > 0:
            btn_text = f"🟢 {pl['name']} — ${float(pl['price']):.2f} (📦 {stock}){warranty_badge}"
            cb = f"order_start_{pl['id']}"
        else:
            btn_text = f"❌ {pl['name']} — ${float(pl['price']):.2f} (Sold Out){warranty_badge}"
            cb = f"notify_req_{pl['id']}"
        keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=cb)])

    keyboard.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data=f"prod_view_{product_id}"),
        InlineKeyboardButton(text="◀️ Store", callback_data="cust_store_p0")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- Admin Keyboards ---

def get_admin_dashboard_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Dashboard", callback_data="admin_stats"),
         InlineKeyboardButton(text="🛍 Products", callback_data="admin_prods_p0")],
        [InlineKeyboardButton(text="📦 Inventory", callback_data="admin_inv_root"),
         InlineKeyboardButton(text="🧾 Orders", callback_data="admin_orders_menu")],
        [InlineKeyboardButton(text="💳 Pending Deposits", callback_data="admin_deposits_list")],
        [InlineKeyboardButton(text="👥 Users", callback_data="admin_users"),
         InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="⚙️ Settings", callback_data="admin_settings"),
         InlineKeyboardButton(text="📝 Logs", callback_data="admin_logs")],
        [InlineKeyboardButton(text="🛍 OPEN STORE", callback_data="cust_home")]
    ])


# =============================================================================
# 6. HANDLERS: START, NAVIGATION & HELP
# =============================================================================

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await register_or_update_user(message.from_user)

    if is_admin(message.from_user.id):
        welcome_admin = (
            "👑 <b>Welcome Admin</b>\n\n"
            "ISell Store Administration System.\n"
            "Manage products, plans, inventory, customer orders, deposits, and broadcasts."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ ADMIN PANEL", callback_data="admin_dashboard")],
            [InlineKeyboardButton(text="🛍 OPEN STORE", callback_data="cust_home")]
        ])
        await message.answer(welcome_admin, reply_markup=kb, parse_mode="HTML")
    else:
        store_name = await get_setting("store_name", DEFAULT_STORE_NAME)
        welcome_text = (
            f"🛍 <b>Welcome to {html.escape(store_name)}</b>\n\n"
            "Your trusted platform for digital products, licenses, accounts, and subscriptions.\n"
            "Deposit funds to your wallet or buy directly with real-time stock availability."
        )
        await message.answer(welcome_text, reply_markup=get_customer_home_kb(message.from_user.id), parse_mode="HTML")

@dp.message(Command("store"))
async def cmd_store(message: Message, state: FSMContext):
    await state.clear()
    products = await get_products_with_stock_info(active_only=True)
    text = "🛍 <b>ISell Digital Catalog</b>\n\nSelect a product to view plans, specifications, and real-time inventory:"
    await message.answer(text, reply_markup=get_products_grid_kb(products, page=0), parse_mode="HTML")

@dp.message(Command("wallet"))
async def cmd_wallet(message: Message, state: FSMContext):
    await state.clear()
    await show_user_wallet(message.from_user.id, target=message)

@dp.message(Command("orders"))
async def cmd_orders(message: Message):
    await show_user_orders(message.from_user.id, target=message)

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    await show_user_profile(message.from_user.id, target=message)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
    text = (
        "ℹ️ <b>ISell Store Help & Guides</b>\n\n"
        "• <b>Wallet:</b> Deposit USDT or Binance Pay funds into your internal balance for 1-click instant checkouts.\n"
        "• <b>How to Buy:</b> Select a product, choose your plan, and pay via Wallet or manual crypto transfer.\n"
        "• <b>Warranty:</b> Each plan displays exact warranty terms (e.g., 30 Days, 2 Months, Lifetime Replacement).\n"
        "• <b>Stock Alerts:</b> If an item is sold out, click 🔔 Notify Me to be notified on restock.\n\n"
        f"Need assistance? Reach out to support: @{support_user}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Store", callback_data="cust_store_p0"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_home")
async def cb_cust_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    store_name = await get_setting("store_name", DEFAULT_STORE_NAME)
    text = (
        f"🛍 <b>Welcome to {html.escape(store_name)}</b>\n\n"
        "Instant delivery digital subscriptions, accounts, and tools."
    )
    await call.message.edit_text(text, reply_markup=get_customer_home_kb(call.from_user.id), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("cust_store_p"))
async def cb_cust_store_pagination(call: CallbackQuery):
    page = int(call.data.replace("cust_store_p", ""))
    products = await get_products_with_stock_info(active_only=True)
    text = (
        "🛍 <b>ISell Digital Catalog</b>\n\n"
        "🟢 <i>Available</i> | ❌ <i>Sold Out</i>\n"
        "Click any product below to explore plans and details:"
    )
    await call.message.edit_text(text, reply_markup=get_products_grid_kb(products, page=page), parse_mode="HTML")
    await call.answer()


# =============================================================================
# 7. WALLET SYSTEM (DEPOSITS & BALANCE)
# =============================================================================

async def show_user_wallet(user_id: int, target: Any):
    user = await get_user(user_id)
    balance = float(user["wallet_balance"]) if user and user.get("wallet_balance") is not None else 0.00
    currency = await get_setting("currency", DEFAULT_CURRENCY)

    text = (
        "💳 <b>YOUR STORE WALLET</b>\n\n"
        f"<b>Current Balance:</b> <code>${balance:.2f} {currency}</code>\n\n"
        "💡 <i>You can deposit funds into your wallet anytime. "
        "Orders paid via wallet balance are confirmed and fulfilled instantly!</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Deposit Funds", callback_data="wallet_deposit_start")],
        [InlineKeyboardButton(text="🛍 Shop with Balance", callback_data="cust_store_p0")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="cust_wallet"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_wallet")
async def cb_cust_wallet(call: CallbackQuery):
    await show_user_wallet(call.from_user.id, target=call)
    await call.answer()

@dp.callback_query(F.data == "wallet_deposit_start")
async def cb_wallet_deposit_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(CustomerDepositFSM.waiting_for_amount)
    prompt = (
        "➕ <b>DEPOSIT FUNDS TO WALLET</b>\n\n"
        "Please enter the amount in USD/USDT you wish to deposit (e.g. <code>20.00</code>):"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cust_wallet")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CustomerDepositFSM.waiting_for_amount)
async def handle_deposit_amount(message: Message, state: FSMContext):
    try:
        raw_val = message.text.strip().replace("$", "")
        amount = float(raw_val)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Please enter a valid positive number (e.g. <code>15.00</code>):", parse_mode="HTML")
        return

    await state.update_data(amount=amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟡 Binance ID (Internal)", callback_data="dep_method_BINANCE")],
        [InlineKeyboardButton(text="₮ USDT (BEP-20)", callback_data="dep_method_USDT_BEP20")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cust_wallet")]
    ])
    await message.answer(
        f"💳 <b>Select Deposit Method for ${amount:.2f} USDT:</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("dep_method_"))
async def cb_dep_method(call: CallbackQuery, state: FSMContext):
    method = call.data.replace("dep_method_", "")
    data = await state.get_data()
    amount = data.get("amount", 0.0)

    binance_id = await get_setting("binance_id", DEFAULT_BINANCE_ID)
    usdt_addr = await get_setting("usdt_bep20", DEFAULT_USDT_ADDR)

    if method == "BINANCE":
        instr = (
            "🟡 <b>BINANCE PAY DEPOSIT</b>\n\n"
            f"<b>Payee Binance ID:</b> <code>{binance_id}</code>\n"
            f"<b>Amount:</b> <code>${amount:.2f} USDT</code>\n\n"
            "<i>Instructions:</i>\n"
            "1. Send the exact amount using Binance Pay.\n"
            "2. Note your Binance Order ID / TxID.\n"
            "3. Click <b>I Have Paid</b> below to upload proof."
        )
    else:
        instr = (
            "₮ <b>USDT BEP-20 DEPOSIT</b>\n\n"
            f"<b>Network:</b> BSC (BNB Smart Chain / BEP-20)\n"
            f"<b>Deposit Address:</b>\n<code>{usdt_addr}</code>\n\n"
            f"<b>Amount:</b> <code>${amount:.2f} USDT</code>\n\n"
            "<i>Instructions:</i>\n"
            "1. Send the exact USDT via BEP-20 network.\n"
            "2. Note your transaction hash / take screenshot.\n"
            "3. Click <b>I Have Paid</b> below to upload proof."
        )

    await state.update_data(method=method)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I Have Paid", callback_data="dep_submit_proof_prompt")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cust_wallet")]
    ])
    await call.message.edit_text(instr, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "dep_submit_proof_prompt")
async def cb_dep_submit_proof_prompt(call: CallbackQuery, state: FSMContext):
    await state.set_state(CustomerDepositFSM.waiting_for_proof)
    prompt = (
        "📝 <b>SUBMIT DEPOSIT PROOF</b>\n\n"
        "Send your <b>Transaction Hash / ID</b> as a message, OR send a <b>Screenshot</b> of your transfer:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cust_wallet")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CustomerDepositFSM.waiting_for_proof)
async def handle_deposit_proof_submission(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("amount", 0.0)
    method = data.get("method", "MANUAL")

    photo_id = ""
    tx_id = ""
    if message.photo:
        photo_id = message.photo[-1].file_id
        tx_id = message.caption or "Screenshot attached"
    elif message.text:
        tx_id = message.text.strip()
    else:
        await message.answer("Please send a valid transaction hash or payment screenshot.")
        return

    # Record deposit in DB
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO deposits (user_id, amount, payment_method, tx_id, proof_file_id, status) "
                "VALUES (%s, %s, %s, %s, %s, 'PENDING')",
                (message.from_user.id, amount, method, tx_id, photo_id)
            )
            deposit_id = cur.lastrowid

    await state.clear()
    await message.answer(
        f"✅ <b>Deposit Request #{deposit_id} Submitted!</b>\n\n"
        f"Amount: <b>${amount:.2f} USDT</b>\n"
        "Our team will verify the payment and credit your wallet shortly.",
        reply_markup=get_customer_home_kb(message.from_user.id),
        parse_mode="HTML"
    )

    # Notify Admin
    admin_text = (
        "━━━━━━━━━━━━━━━━\n"
        "💳 <b>NEW WALLET DEPOSIT REQUEST</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"<b>Deposit ID:</b> <code>#{deposit_id}</code>\n"
        f"<b>User:</b> @{html.escape(message.from_user.username or 'N/A')} (<code>{message.from_user.id}</code>)\n"
        f"<b>Amount:</b> <b>${amount:.2f} USDT</b>\n"
        f"<b>Method:</b> {method}\n"
        f"<b>Tx ID / Note:</b> <code>{html.escape(tx_id)}</code>\n"
    )
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ APPROVE DEPOSIT", callback_data=f"adm_dep_app_{deposit_id}"),
         InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_dep_rej_{deposit_id}")]
    ])

    try:
        if photo_id:
            await bot.send_photo(chat_id=ADMIN_ID, photo=photo_id, caption=admin_text, reply_markup=admin_kb, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=ADMIN_ID, text=admin_text, reply_markup=admin_kb, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to notify admin of deposit: %s", e)


# =============================================================================
# 8. PRODUCT & PLAN VIEWS WITH DETAILED SPECS & WARRANTY
# =============================================================================

@dp.callback_query(F.data.startswith("prod_view_"))
async def cb_prod_view(call: CallbackQuery):
    product_id = int(call.data.split("_")[2])
    product = await get_product_by_id(product_id)
    if not product:
        await call.answer("Product no longer exists.", show_alert=True)
        return

    plans = await get_plans_by_product_id(product_id, active_only=True)
    emoji = product.get("emoji") or "📦"
    text = f"{emoji} <b>{html.escape(product['name']).upper()}</b>\n"
    if product.get("description"):
        text += f"<i>{html.escape(product['description'])}</i>\n\n"
    else:
        text += "\n"

    if not plans:
        text += "❌ <b>Currently Sold Out</b>\nNo active tiers or plans have been configured for this product yet."
    else:
        text += "<b>Available Plans & Specifications:</b>\n\n"
        for pl in plans:
            stock = pl["available_count"]
            st_badge = "🟢 In Stock" if stock > 0 else "❌ Sold Out"
            warranty = html.escape(pl.get("warranty") or "30 Days")
            details = html.escape(pl.get("details") or "Standard tier subscription.")
            text += (
                f"• <b>{html.escape(pl['name'])}</b> — ${float(pl['price']):.2f}\n"
                f"  └ <i>Specs:</i> {details}\n"
                f"  └ <i>Warranty:</i> 🛡 <b>{warranty}</b> | <i>Stock:</i> {st_badge} ({stock})\n\n"
            )
        text += "Select a plan button below to proceed:"

    await call.message.edit_text(text, reply_markup=get_plans_kb(product_id, plans), parse_mode="HTML")
    await call.answer()


# =============================================================================
# 9. STOCK NOTIFICATIONS ("NOTIFY ME")
# =============================================================================

@dp.callback_query(F.data.startswith("notify_req_"))
async def cb_notify_request(call: CallbackQuery):
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)
    if not plan:
        await call.answer("Plan not found.", show_alert=True)
        return

    await subscribe_stock_notification(call.from_user.id, plan_id)
    await call.answer(
        f"🔔 You will be alerted immediately when {plan['name']} is back in stock!",
        show_alert=True
    )


# =============================================================================
# 10. PURCHASE WORKFLOW (WALLET OR MANUAL CRYPTO)
# =============================================================================

@dp.callback_query(F.data.startswith("order_start_"))
async def cb_order_start(call: CallbackQuery):
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)
    if not plan or plan["available_count"] <= 0:
        await call.answer("Sorry, this plan is currently sold out!", show_alert=True)
        return

    order_id = await create_order(call.from_user.id, plan_id)
    if not order_id:
        await call.answer("Failed to initiate order.", show_alert=True)
        return

    user = await get_user(call.from_user.id)
    balance = float(user["wallet_balance"]) if user and user.get("wallet_balance") is not None else 0.0
    price = float(plan["price"])

    text = (
        "🛒 <b>ORDER CONFIRMATION</b>\n\n"
        f"<b>Product:</b> {html.escape(plan['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(plan['name'])}\n"
        f"<b>Details:</b> <i>{html.escape(plan.get('details') or 'Standard')}</i>\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(plan.get('warranty') or '30 Days')}</b>\n"
        f"<b>Total Price:</b> <code>${price:.2f} USDT</code>\n"
        f"<b>Your Wallet:</b> <code>${balance:.2f} USDT</code>\n\n"
        "Choose your preferred payment method:"
    )

    kb_rows = []
    # Wallet payment option
    if balance >= price:
        kb_rows.append([InlineKeyboardButton(text=f"⚡ Pay with Wallet (${balance:.2f})", callback_data=f"order_pay_wallet_{order_id}")])
    else:
        kb_rows.append([InlineKeyboardButton(text=f"💳 Deposit to Wallet (Have ${balance:.2f})", callback_data="wallet_deposit_start")])

    # Manual Direct Crypto
    kb_rows.append([InlineKeyboardButton(text="🟡 Binance ID (Manual)", callback_data=f"order_pay_binance_{order_id}")])
    kb_rows.append([InlineKeyboardButton(text="₮ USDT BEP-20 (Manual)", callback_data=f"order_pay_usdt_{order_id}")])
    kb_rows.append([InlineKeyboardButton(text="❌ Cancel Order", callback_data=f"order_cancel_{order_id}")])

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("order_pay_wallet_"))
async def cb_order_pay_wallet(call: CallbackQuery):
    order_id = int(call.data.split("_")[3])
    success, delivery_or_err = await process_wallet_purchase(order_id, call.from_user.id)
    if not success:
        await call.answer(delivery_or_err, show_alert=True)
        return

    order = await get_order_by_id(order_id)
    delivery_text = (
        "✅ <b>ORDER COMPLETED & DELIVERED</b>\n\n"
        f"<b>Order ID:</b> <code>#{order_id}</code>\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(order.get('warranty') or '30 Days')}</b>\n"
        f"<b>Paid with:</b> Store Wallet Balance\n\n"
        "📦 <b>Credentials / Delivery:</b>\n"
        f"<code>{html.escape(delivery_or_err)}</code>\n\n"
        "Thank you for purchasing with ISell Store!"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 My Orders", callback_data="cust_my_orders"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(delivery_text, reply_markup=kb, parse_mode="HTML")
    await call.answer("Order paid and delivered successfully!", show_alert=True)

@dp.callback_query(F.data.startswith("order_pay_"))
async def cb_order_pay_direct(call: CallbackQuery):
    parts = call.data.split("_")
    method = parts[2]
    order_id = int(parts[3])

    order = await get_order_by_id(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Invalid order.", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET payment_method = %s WHERE id = %s", (method.upper(), order_id))

    binance_id = await get_setting("binance_id", DEFAULT_BINANCE_ID)
    usdt_addr = await get_setting("usdt_bep20", DEFAULT_USDT_ADDR)

    if method == "binance":
        instr = (
            "🟡 <b>BINANCE TRANSFER DETAILS</b>\n\n"
            f"<b>Payee Binance ID:</b> <code>{binance_id}</code>\n"
            f"<b>Amount:</b> <code>{float(order['amount']):.2f}</code> USDT\n\n"
            "<i>Instructions:</i>\n"
            "1. Send the exact amount via Binance App.\n"
            "2. Note your Transaction ID or take a screenshot.\n"
            "3. Click <b>I Have Paid</b> below."
        )
    else:
        instr = (
            "₮ <b>USDT BEP-20 TRANSFER DETAILS</b>\n\n"
            f"<b>Network:</b> BSC (BNB Smart Chain / BEP-20)\n"
            f"<b>Deposit Address:</b>\n<code>{usdt_addr}</code>\n\n"
            f"<b>Amount:</b> <code>{float(order['amount']):.2f}</code> USDT\n\n"
            "<i>Instructions:</i>\n"
            "1. Send the exact USDT to the address above.\n"
            "2. Note down your TxHash / take a screenshot.\n"
            "3. Click <b>I Have Paid</b> below."
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I Have Paid", callback_data=f"order_proof_{order_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data=f"order_start_{order['plan_id']}")]
    ])
    await call.message.edit_text(instr, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("order_proof_"))
async def cb_order_proof_prompt(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split("_")[2])
    order = await get_order_by_id(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Order not found.", show_alert=True)
        return

    await state.set_state(CustomerPurchaseFSM.waiting_for_payment_proof)
    await state.update_data(order_id=order_id)

    prompt = (
        f"📝 <b>SUBMIT PAYMENT PROOF FOR ORDER #{order_id}</b>\n\n"
        "Send your <b>Transaction ID / Hash</b> as a message, OR send a <b>Payment Screenshot</b> photo:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data=f"order_cancel_{order_id}")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CustomerPurchaseFSM.waiting_for_payment_proof)
async def handle_payment_proof_submission(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    if not order_id:
        await state.clear()
        return

    tx_id = ""
    photo_file_id = ""

    if message.photo:
        photo_file_id = message.photo[-1].file_id
        tx_id = message.caption or "Screenshot attached"
    elif message.text:
        tx_id = message.text.strip()
    else:
        await message.answer("Please send a valid text Transaction ID or screenshot.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE orders SET status = 'PAYMENT_SUBMITTED', tx_id = %s, proof_file_id = %s WHERE id = %s",
                (tx_id, photo_file_id, order_id)
            )

    await state.clear()
    order = await get_order_by_id(order_id)
    await message.answer(
        f"✅ <b>Payment Submitted!</b>\n\n"
        f"Your proof for Order <code>#{order_id}</code> is received and pending admin approval.\n"
        "You will be notified right here once approved and delivered.",
        reply_markup=get_customer_home_kb(message.from_user.id),
        parse_mode="HTML"
    )

    # Admin notification
    admin_notif_text = (
        "━━━━━━━━━━━━━━━━\n"
        "💰 <b>NEW ORDER PAYMENT SUBMITTED</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Customer:</b> @{html.escape(order['username'] or 'unknown')} (<code>{order['user_id']}</code>)\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Warranty:</b> 🛡 {html.escape(order.get('warranty') or '30 Days')}\n"
        f"<b>Amount:</b> ${float(order['amount']):.2f} {order['currency']}\n"
        f"<b>Method:</b> {order['payment_method']}\n"
        f"<b>Tx ID / Note:</b> <code>{html.escape(order['tx_id'] or 'None')}</code>\n"
    )
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ APPROVE", callback_data=f"adm_pay_approve_{order_id}"),
         InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_pay_reject_{order_id}")],
        [InlineKeyboardButton(text="🔎 VIEW ORDER", callback_data=f"adm_order_view_{order_id}")]
    ])

    try:
        if order["proof_file_id"]:
            await bot.send_photo(chat_id=ADMIN_ID, photo=order["proof_file_id"], caption=admin_notif_text, reply_markup=admin_kb, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=ADMIN_ID, text=admin_notif_text, reply_markup=admin_kb, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to notify admin of payment: %s", e)

@dp.callback_query(F.data.startswith("order_cancel_"))
async def cb_order_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    order_id = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE orders SET status = 'CANCELLED' WHERE id = %s AND user_id = %s AND status = 'PENDING_PAYMENT'",
                (order_id, call.from_user.id)
            )
    await call.message.edit_text("❌ Order cancelled.", reply_markup=get_customer_home_kb(call.from_user.id))
    await call.answer()


# =============================================================================
# 11. SEARCH, USER PROFILE & ORDERS
# =============================================================================

@dp.callback_query(F.data == "cust_search")
async def cb_cust_search(call: CallbackQuery, state: FSMContext):
    await state.set_state(CustomerSearchFSM.waiting_for_query)
    await call.message.edit_text("🔎 <b>Product Search</b>\n\nEnter product name or keyword:", parse_mode="HTML")
    await call.answer()

@dp.message(CustomerSearchFSM.waiting_for_query)
async def handle_customer_search_query(message: Message, state: FSMContext):
    await state.clear()
    query = message.text.strip().lower()
    all_products = await get_products_with_stock_info(active_only=True)
    matches = [p for p in all_products if query in p["name"].lower()]

    if not matches:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Browse Catalog", callback_data="cust_store_p0"),
             InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
        ])
        await message.answer(f"❌ No matching products found for '<b>{html.escape(query)}</b>'.", reply_markup=kb, parse_mode="HTML")
        return

    kb = get_products_grid_kb(matches, page=0, per_page=9)
    await message.answer(f"🔎 <b>Search Results for:</b> <i>{html.escape(query)}</i>", reply_markup=kb, parse_mode="HTML")

async def show_user_orders(user_id: int, target: Any):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT o.id, o.amount, o.status, o.created_at, p.name AS product_name, pl.name AS plan_name, pl.warranty
                FROM orders o
                JOIN products p ON p.id = o.product_id
                JOIN plans pl ON pl.id = o.plan_id
                WHERE o.user_id = %s
                ORDER BY o.id DESC LIMIT 10;
            """, (user_id,))
            orders = await cur.fetchall()

    if not orders:
        text = "📦 <b>My Orders</b>\n\nYou haven't placed any orders yet."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Visit Store", callback_data="cust_store_p0")]
        ])
    else:
        text = "📦 <b>Your Recent Orders:</b>\n\n"
        for o in orders:
            status_emoji = "⏳" if "PAYMENT" in o["status"] else "✅" if o["status"] == "DELIVERED" else "⚙️"
            text += (
                f"{status_emoji} <b>Order #{o['id']}</b> — {html.escape(o['product_name'])} ({html.escape(o['plan_name'])})\n"
                f"Status: <code>{o['status']}</code> | ${float(o['amount']):.2f}\n"
                f"Warranty: 🛡 <b>{html.escape(o.get('warranty') or '30 Days')}</b>\n"
                f"Date: {o['created_at'].strftime('%Y-%m-%d %H:%M')}\n\n"
            )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="cust_my_orders"),
             InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
        ])

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_my_orders")
async def cb_cust_my_orders(call: CallbackQuery):
    await show_user_orders(call.from_user.id, target=call)
    await call.answer()

async def show_user_profile(user_id: int, target: Any):
    user = await get_user(user_id)
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0) AS total_spent FROM orders WHERE user_id = %s AND status = 'DELIVERED'", (user_id,))
            stats = await cur.fetchone()

    total_orders = stats["cnt"] if stats else 0
    total_spent = stats["total_spent"] if stats else 0
    balance = float(user["wallet_balance"]) if user and user.get("wallet_balance") is not None else 0.0

    text = (
        "👤 <b>USER PROFILE</b>\n\n"
        f"<b>Telegram ID:</b> <code>{user_id}</code>\n"
        f"<b>Username:</b> @{user['username'] if user and user['username'] else 'N/A'}\n"
        f"<b>Wallet Balance:</b> <code>${balance:.2f} USDT</code>\n"
        f"<b>Completed Orders:</b> {total_orders}\n"
        f"<b>Total Volume:</b> ${float(total_spent):.2f} USDT\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Manage Wallet", callback_data="cust_wallet")],
        [InlineKeyboardButton(text="📦 My Orders", callback_data="cust_my_orders"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_profile")
async def cb_cust_profile(call: CallbackQuery):
    await show_user_profile(call.from_user.id, target=call)
    await call.answer()

@dp.callback_query(F.data == "cust_support")
async def cb_cust_support(call: CallbackQuery):
    support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
    text = (
        "📞 <b>Customer Support Desk</b>\n\n"
        "Need assistance with warranty claims, custom subscriptions, or wallet top-ups?\n\n"
        f"Direct Telegram: @{support_user}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Message Support", url=f"https://t.me/{support_user}")],
        [InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "cust_notifs")
async def cb_cust_notifs(call: CallbackQuery):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT p.name AS product_name, pl.name AS plan_name
                FROM stock_notifications sn
                JOIN plans pl ON pl.id = sn.plan_id
                JOIN products p ON p.id = pl.product_id
                WHERE sn.user_id = %s
            """, (call.from_user.id,))
            subs = await cur.fetchall()

    if not subs:
        text = "🔔 <b>Stock Alerts</b>\n\nYou have no active back-in-stock subscriptions."
    else:
        text = "🔔 <b>Your Active Stock Subscriptions:</b>\n\n"
        for s in subs:
            text += f"• {html.escape(s['product_name'])} — {html.escape(s['plan_name'])}\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Store", callback_data="cust_store_p0"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "cust_help")
async def cb_cust_help(call: CallbackQuery):
    await cmd_help(call.message)
    await call.answer()


# =============================================================================
# 12. ADMIN CONTROL PANEL & DASHBOARD
# =============================================================================

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("⚙️ <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_dashboard_kb(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_dashboard")
async def cb_admin_dashboard(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Unauthorized", show_alert=True)
        return
    await state.clear()
    await call.message.edit_text("⚙️ <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_dashboard_kb(), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) AS total_users, COALESCE(SUM(wallet_balance), 0) AS total_wallets FROM users")
            u_row = await cur.fetchone()

            await cur.execute("SELECT COUNT(*) AS total_orders FROM orders")
            total_orders = (await cur.fetchone())["total_orders"]

            await cur.execute("SELECT COUNT(*) AS pending FROM orders WHERE status = 'PAYMENT_SUBMITTED'")
            pending = (await cur.fetchone())["pending"]

            await cur.execute("SELECT COUNT(*) AS pending_dep FROM deposits WHERE status = 'PENDING'")
            pending_dep = (await cur.fetchone())["pending_dep"]

            await cur.execute("SELECT COUNT(*) AS delivered FROM orders WHERE status = 'DELIVERED'")
            completed = (await cur.fetchone())["delivered"]

            await cur.execute("SELECT COALESCE(SUM(amount), 0) AS revenue FROM orders WHERE status = 'DELIVERED'")
            revenue = (await cur.fetchone())["revenue"]

            await cur.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN i.status = 'AVAILABLE' THEN pl.id END) AS available_plans,
                    COUNT(DISTINCT CASE WHEN i.id IS NULL OR i.status != 'AVAILABLE' THEN pl.id END) AS sold_out_plans
                FROM plans pl
                LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
                WHERE pl.is_active = 1;
            """)
            plan_stats = await cur.fetchone()

    stats_text = (
        "📊 <b>DASHBOARD STATISTICS</b>\n\n"
        f"👥 <b>Total Users:</b> {u_row['total_users']}\n"
        f"💳 <b>User Wallets Combined:</b> ${float(u_row['total_wallets']):,.2f} USDT\n"
        f"🧾 <b>Total Orders:</b> {total_orders}\n"
        f"⏳ <b>Pending Order Payments:</b> {pending}\n"
        f"💳 <b>Pending Wallet Deposits:</b> {pending_dep}\n"
        f"✅ <b>Completed Orders:</b> {completed}\n"
        f"💰 <b>Total Sales Revenue:</b> ${float(revenue):,.2f} USDT\n\n"
        f"📦 <b>Available Plans:</b> {plan_stats['available_plans'] or 0}\n"
        f"❌ <b>Sold Out Plans:</b> {plan_stats['sold_out_plans'] or 0}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_stats")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(stats_text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =============================================================================
# 13. ADMIN: WALLET DEPOSIT APPROVAL / REJECTION
# =============================================================================

@dp.callback_query(F.data == "admin_deposits_list")
async def cb_admin_deposits_list(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT d.*, u.username 
                FROM deposits d
                JOIN users u ON u.id = d.user_id
                WHERE d.status = 'PENDING'
                ORDER BY d.id DESC LIMIT 15;
            """)
            deps = await cur.fetchall()

    if not deps:
        text = "💳 <b>PENDING WALLET DEPOSITS</b>\n\nNo pending deposits waiting for review."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
        ])
    else:
        text = "💳 <b>PENDING WALLET DEPOSITS:</b>\nSelect a deposit to approve or reject:"
        kb_rows = []
        for d in deps:
            btn_text = f"#{d['id']} - ${float(d['amount']):.2f} (@{d['username'] or d['user_id']})"
            kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"adm_dep_view_{d['id']}")])
        kb_rows.append([InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_dep_view_"))
async def cb_adm_dep_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    dep_id = int(call.data.split("_")[3])
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT d.*, u.username FROM deposits d JOIN users u ON u.id = d.user_id WHERE d.id = %s", (dep_id,))
            dep = await cur.fetchone()

    if not dep:
        await call.answer("Deposit not found.", show_alert=True)
        return

    text = (
        f"💳 <b>DEPOSIT #{dep['id']} REVIEW</b>\n\n"
        f"<b>User:</b> @{html.escape(dep['username'] or 'N/A')} (<code>{dep['user_id']}</code>)\n"
        f"<b>Amount:</b> <b>${float(dep['amount']):.2f} USDT</b>\n"
        f"<b>Method:</b> {dep['payment_method']}\n"
        f"<b>Tx ID / Hash:</b> <code>{html.escape(dep['tx_id'] or 'None')}</code>\n"
        f"<b>Status:</b> <code>{dep['status']}</code>\n"
        f"<b>Date:</b> {dep['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ APPROVE & CREDIT", callback_data=f"adm_dep_app_{dep['id']}"),
         InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_dep_rej_{dep['id']}")],
        [InlineKeyboardButton(text="◀️ Pending Deposits", callback_data="admin_deposits_list")]
    ])

    if dep.get("proof_file_id"):
        try:
            await call.message.delete()
            await bot.send_photo(chat_id=call.from_user.id, photo=dep["proof_file_id"], caption=text, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_dep_app_"))
async def cb_adm_dep_app(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    dep_id = int(call.data.split("_")[3])

    async with db_pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM deposits WHERE id = %s FOR UPDATE", (dep_id,))
                dep = await cur.fetchone()
                if not dep or dep["status"] != "PENDING":
                    await conn.rollback()
                    await call.answer("Deposit already processed.", show_alert=True)
                    return

                # Credit user balance
                await cur.execute(
                    "UPDATE users SET wallet_balance = wallet_balance + %s WHERE id = %s",
                    (dep["amount"], dep["user_id"])
                )
                await cur.execute("UPDATE deposits SET status = 'APPROVED', reviewed_by = %s WHERE id = %s", (call.from_user.id, dep_id))

            await conn.commit()
        except Exception as e:
            await conn.rollback()
            await call.answer(f"Error: {e}", show_alert=True)
            return

    await log_admin_action(call.from_user.id, "Deposit Approved", f"Dep ID: {dep_id}, Amount: ${float(dep['amount']):.2f}")

    # Notify customer
    try:
        await bot.send_message(
            chat_id=dep["user_id"],
            text=(
                f"✅ <b>DEPOSIT CONFIRMED!</b>\n\n"
                f"Your deposit of <b>${float(dep['amount']):.2f} USDT</b> has been approved and credited to your wallet!\n"
                "You can now use your wallet for 1-click checkouts in the store."
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    await call.message.edit_text(
        f"✅ Deposit #{dep_id} approved! <b>${float(dep['amount']):.2f}</b> added to user <code>{dep['user_id']}</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Deposits", callback_data="admin_deposits_list")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_dep_rej_"))
async def cb_adm_dep_rej(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    dep_id = int(call.data.split("_")[3])

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM deposits WHERE id = %s", (dep_id,))
            dep = await cur.fetchone()
            if not dep or dep["status"] != "PENDING":
                await call.answer("Deposit already processed.", show_alert=True)
                return
            await cur.execute("UPDATE deposits SET status = 'REJECTED', reviewed_by = %s WHERE id = %s", (call.from_user.id, dep_id))

    await log_admin_action(call.from_user.id, "Deposit Rejected", f"Dep ID: {dep_id}")

    try:
        support_u = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
        await bot.send_message(
            chat_id=dep["user_id"],
            text=f"❌ Your wallet deposit request <code>#{dep_id}</code> was rejected. If you feel this is a mistake, contact @{support_u}.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await call.message.edit_text(
        f"❌ Deposit #{dep_id} rejected.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Deposits", callback_data="admin_deposits_list")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()


# =============================================================================
# 14. ADMIN: PRODUCT MANAGEMENT
# =============================================================================

@dp.callback_query(F.data.startswith("admin_prods_p"))
async def cb_admin_products(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Unauthorized", show_alert=True)
        return

    page = int(call.data.replace("admin_prods_p", ""))
    products = await get_products_with_stock_info(active_only=False)
    per_page = 8
    total_pages = max(1, math.ceil(len(products) / per_page))
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    sliced = products[start:start + per_page]

    kb_rows = []
    for p in sliced:
        status_flag = "🟢" if p["is_active"] else "⏸"
        stock_flag = "📦" if p["available_stock_count"] > 0 else "❌"
        btn_text = f"{status_flag}{stock_flag} {p['emoji']} {p['name']}"
        kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"adm_pdetail_{p['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_prods_p{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_prods_p{page+1}"))
    kb_rows.append(nav)

    kb_rows.append([InlineKeyboardButton(text="➕ Add Product", callback_data="adm_padd")])
    kb_rows.append([InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")])

    await call.message.edit_text("🛍 <b>PRODUCT CATALOG MANAGEMENT</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "adm_padd")
async def cb_adm_padd(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminProductFSM.waiting_for_name)
    await call.message.edit_text("➕ <b>New Product</b>\nEnter the product title (e.g. <code>ChatGPT</code>):", parse_mode="HTML")
    await call.answer()

@dp.message(AdminProductFSM.waiting_for_name)
async def handle_padd_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminProductFSM.waiting_for_emoji)
    await message.answer("Enter an emoji for this product (e.g. 🤖):")

@dp.message(AdminProductFSM.waiting_for_emoji)
async def handle_padd_emoji(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(emoji=message.text.strip())
    await state.set_state(AdminProductFSM.waiting_for_description)
    await message.answer("Enter product description / overview:")

@dp.message(AdminProductFSM.waiting_for_description)
async def handle_padd_desc(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    desc = message.text.strip()
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO products (name, emoji, description, is_active) VALUES (%s, %s, %s, 1)",
                (data["name"], data["emoji"], desc)
            )
            new_id = cur.lastrowid

    await log_admin_action(message.from_user.id, "Product Created", f"ID: {new_id}, Name: {data['name']}")
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Plan Now", callback_data=f"adm_plan_add_{new_id}")],
        [InlineKeyboardButton(text="🛍 Product List", callback_data="admin_prods_p0")]
    ])
    await message.answer(f"✅ Product <b>{html.escape(data['name'])}</b> created with <b>NO STOCK</b>.", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("adm_pdetail_"))
async def cb_adm_pdetail(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    p = await get_product_by_id(pid)
    plans = await get_plans_by_product_id(pid, active_only=False)

    text = (
        f"{p['emoji']} <b>{html.escape(p['name'])}</b> (ID: #{p['id']})\n"
        f"<b>Status:</b> {'🟢 Active' if p['is_active'] else '⏸ Disabled'}\n"
        f"<b>Description:</b> {html.escape(p['description'] or 'None')}\n\n"
        f"<b>Configured Plans ({len(plans)}):</b>\n"
    )
    for pl in plans:
        text += f"• {pl['name']} - ${float(pl['price']):.2f} (🛡 {pl.get('warranty') or '30D'} | 📦 {pl['available_count']})\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Plan", callback_data=f"adm_plan_add_{pid}"),
         InlineKeyboardButton(text="📋 Manage Plans", callback_data=f"adm_plans_list_{pid}")],
        [InlineKeyboardButton(text="🔄 Toggle Active", callback_data=f"adm_ptoggle_{pid}"),
         InlineKeyboardButton(text="🗑 Delete Product", callback_data=f"adm_pdel_{pid}")],
        [InlineKeyboardButton(text="◀️ Products", callback_data="admin_prods_p0")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_ptoggle_"))
async def cb_adm_ptoggle(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE products SET is_active = NOT is_active WHERE id = %s", (pid,))
    await cb_adm_pdetail(call)

@dp.callback_query(F.data.startswith("adm_pdel_"))
async def cb_adm_pdel(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM products WHERE id = %s", (pid,))
    await log_admin_action(call.from_user.id, "Product Deleted", f"Product ID: {pid}")
    await call.answer("Product deleted.", show_alert=True)
    call.data = "admin_prods_p0"
    await cb_admin_products(call)


# =============================================================================
# 15. ADMIN: PLAN MANAGEMENT (CUSTOM DETAILS & TEXT WARRANTY)
# =============================================================================

@dp.callback_query(F.data.startswith("adm_plans_list_"))
async def cb_adm_plans_list(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[3])
    p = await get_product_by_id(pid)
    plans = await get_plans_by_product_id(pid, active_only=False)

    rows = []
    for pl in plans:
        st = "🟢" if pl["is_active"] else "⏸"
        rows.append([InlineKeyboardButton(
            text=f"{st} {pl['name']} - ${float(pl['price']):.2f} (🛡 {pl.get('warranty') or '30D'} | 📦 {pl['available_count']})",
            callback_data=f"adm_plandetail_{pl['id']}"
        )])

    rows.append([InlineKeyboardButton(text="➕ Add New Plan", callback_data=f"adm_plan_add_{pid}")])
    rows.append([InlineKeyboardButton(text="◀️ Product Detail", callback_data=f"adm_pdetail_{pid}")])

    await call.message.edit_text(
        f"📋 <b>PLANS FOR {html.escape(p['name'].upper())}</b>\nSelect a plan to edit or manage inventory:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_plan_add_"))
async def cb_adm_plan_add(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[3])
    await state.set_state(AdminPlanFSM.waiting_for_name)
    await state.update_data(product_id=pid)
    await call.message.edit_text("Enter Plan Name (e.g. <code>GPT TEAM 1 Month</code>):", parse_mode="HTML")
    await call.answer()

@dp.message(AdminPlanFSM.waiting_for_name)
async def handle_plan_add_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPlanFSM.waiting_for_price)
    await message.answer("Enter Price in USD/USDT (e.g. <code>15.00</code>):", parse_mode="HTML")

@dp.message(AdminPlanFSM.waiting_for_price)
async def handle_plan_add_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        price = float(message.text.strip().replace("$", ""))
    except ValueError:
        await message.answer("Invalid price. Enter a number like <code>15.00</code>:", parse_mode="HTML")
        return
    await state.update_data(price=price)
    await state.set_state(AdminPlanFSM.waiting_for_details)
    await message.answer(
        "Enter <b>Plan Details / Features</b>:\n"
        "(e.g. <code>Private Account, 5 Devices, 4K UHD, Full Email Access</code>)",
        parse_mode="HTML"
    )

@dp.message(AdminPlanFSM.waiting_for_details)
async def handle_plan_add_details(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(details=message.text.strip())
    await state.set_state(AdminPlanFSM.waiting_for_warranty)
    await message.answer(
        "Enter <b>Warranty Coverage</b> (letters and numbers allowed):\n"
        "(e.g. <code>30 Days Warranty</code>, <code>2 Months Replacement Warranty</code>, or <code>Lifetime Warranty</code>)",
        parse_mode="HTML"
    )

@dp.message(AdminPlanFSM.waiting_for_warranty)
async def handle_plan_add_warranty(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    warranty = message.text.strip()
    data = await state.get_data()
    pid = data["product_id"]

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO plans (product_id, name, details, price, warranty, is_active) VALUES (%s, %s, %s, %s, %s, 1)",
                (pid, data["name"], data["details"], data["price"], warranty)
            )
            plan_id = cur.lastrowid

    await log_admin_action(message.from_user.id, "Plan Created", f"Plan ID: {plan_id}, Name: {data['name']}")
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Stock Now", callback_data=f"adm_stock_add_{plan_id}")],
        [InlineKeyboardButton(text="📋 Plans List", callback_data=f"adm_plans_list_{pid}")]
    ])
    await message.answer(
        f"✅ Plan <b>{html.escape(data['name'])}</b> created successfully!\n\n"
        f"<b>Price:</b> ${data['price']:.2f}\n"
        f"<b>Details:</b> <i>{html.escape(data['details'])}</i>\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(warranty)}</b>\n"
        "Stock is currently <b>0 (Sold Out)</b> until you add inventory items.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("adm_plandetail_"))
async def cb_adm_plandetail(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)

    text = (
        f"⚙️ <b>PLAN SETTINGS: {html.escape(plan['name'])}</b>\n"
        f"Product: {html.escape(plan['product_name'])}\n"
        f"Price: ${float(plan['price']):.2f}\n"
        f"Warranty: 🛡 <b>{html.escape(plan.get('warranty') or '30 Days')}</b>\n"
        f"Details: <i>{html.escape(plan.get('details') or 'None')}</i>\n"
        f"Active: {'🟢 Yes' if plan['is_active'] else '⏸ No'}\n"
        f"Available Stock: <b>{plan['available_count']}</b> units\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Stock", callback_data=f"adm_stock_add_{plan_id}"),
         InlineKeyboardButton(text="📦 View Inventory", callback_data=f"adm_stock_view_{plan_id}")],
        [InlineKeyboardButton(text="🔄 Toggle Active", callback_data=f"adm_plan_toggle_{plan_id}"),
         InlineKeyboardButton(text="🗑 Delete Plan", callback_data=f"adm_plan_del_{plan_id}")],
        [InlineKeyboardButton(text="◀️ Product", callback_data=f"adm_pdetail_{plan['product_id']}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_plan_toggle_"))
async def cb_adm_plan_toggle(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE plans SET is_active = NOT is_active WHERE id = %s", (plan_id,))
    call.data = f"adm_plandetail_{plan_id}"
    await cb_adm_plandetail(call)

@dp.callback_query(F.data.startswith("adm_plan_del_"))
async def cb_adm_plan_del(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM plans WHERE id = %s", (plan_id,))
    await log_admin_action(call.from_user.id, "Plan Deleted", f"Plan ID: {plan_id}")
    await call.answer("Plan deleted.", show_alert=True)
    call.data = f"adm_plans_list_{plan['product_id']}"
    await cb_adm_plans_list(call)


# =============================================================================
# 16. ADMIN: INVENTORY & STOCK RESTOCK
# =============================================================================

@dp.callback_query(F.data == "admin_inv_root")
async def cb_admin_inv_root(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT pl.id, pl.name, p.name AS prod_name,
                       COUNT(CASE WHEN i.status = 'AVAILABLE' THEN 1 END) AS available_count
                FROM plans pl
                JOIN products p ON p.id = pl.product_id
                LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
                GROUP BY pl.id
                ORDER BY p.name ASC;
            """)
            rows = await cur.fetchall()

    kb_rows = []
    for r in rows:
        kb_rows.append([InlineKeyboardButton(
            text=f"{r['prod_name']} - {r['name']} (📦 {r['available_count']})",
            callback_data=f"adm_stock_add_{r['id']}"
        )])

    kb_rows.append([InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")])
    await call.message.edit_text("📦 <b>SELECT PLAN TO RESTOCK:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_stock_add_"))
async def cb_adm_stock_add(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)

    await state.set_state(AdminStockFSM.waiting_for_items)
    await state.update_data(plan_id=plan_id, old_stock=plan["available_count"])

    prompt = (
        f"➕ <b>RESTOCK INVENTORY: {html.escape(plan['name'])}</b>\n"
        f"Current Stock: <b>{plan['available_count']}</b>\n\n"
        "Send your items below, <b>one item per line</b>.\n"
        "Example:\n"
        "<code>user1@mail.com:pass123\nuser2@mail.com:pass456\nKEY-XXXX-YYYY</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data=f"adm_plandetail_{plan_id}")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(AdminStockFSM.waiting_for_items)
async def handle_admin_stock_items(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    plan_id = data["plan_id"]
    old_stock = data.get("old_stock", 0)

    lines = [line.strip() for line in message.text.split("\n") if line.strip()]
    if not lines:
        await message.answer("No valid items detected. Please send lines of credentials or keys.")
        return

    added = await add_stock_items(plan_id, lines, item_type="CODE")
    new_stock = old_stock + added
    plan = await get_plan_by_id(plan_id)

    await log_admin_action(message.from_user.id, "Stock Added", f"Plan ID: {plan_id}, Count: {added}")
    await state.clear()

    if old_stock == 0 and added > 0:
        subs = await get_subscribers_for_plan(plan_id)
        preview_text = (
            "🔥 <b>BACK IN STOCK PREVIEW</b>\n\n"
            f"<b>Product:</b> {html.escape(plan['product_name'])}\n"
            f"<b>Plan:</b> {html.escape(plan['name'])}\n"
            f"<b>Warranty:</b> 🛡 {html.escape(plan.get('warranty') or '30 Days')}\n"
            f"<b>Price:</b> ${float(plan['price']):.2f}\n"
            f"<b>New Stock:</b> {new_stock} Available\n\n"
            f"Interested subscribers waiting: <b>{len(subs)}</b>\n"
            "Dispatch notifications now?"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📢 Notify Interested ({len(subs)})", callback_data=f"adm_notify_subs_{plan_id}")],
            [InlineKeyboardButton(text="📢 Notify All Users", callback_data=f"adm_notify_all_{plan_id}")],
            [InlineKeyboardButton(text="❌ Skip", callback_data=f"adm_plandetail_{plan_id}")]
        ])
        await message.answer(preview_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(
            f"✅ Successfully added <b>{added}</b> inventory items.\nTotal stock: <b>{new_stock}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Plan Detail", callback_data=f"adm_plandetail_{plan_id}")]
            ]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("adm_stock_view_"))
async def cb_adm_stock_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, item_type, content, status FROM inventory WHERE plan_id = %s ORDER BY status ASC, id DESC LIMIT 20",
                (plan_id,)
            )
            items = await cur.fetchall()

    if not items:
        text = "📦 No inventory found for this plan."
    else:
        text = f"📦 <b>Recent Inventory Items (Plan #{plan_id}):</b>\n\n"
        for it in items:
            text += f"• [<code>{it['status']}</code>] {html.escape(it['content'][:40])}...\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Stock", callback_data=f"adm_stock_add_{plan_id}")],
        [InlineKeyboardButton(text="◀️ Plan", callback_data=f"adm_plandetail_{plan_id}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =============================================================================
# 17. RESTOCK NOTIFICATION DISPATCHER
# =============================================================================

@dp.callback_query(F.data.startswith("adm_notify_subs_"))
async def cb_dispatch_subs_notif(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)
    subscribers = await get_subscribers_for_plan(plan_id)

    text = (
        "🔥 <b>BACK IN STOCK!</b>\n\n"
        f"<b>{html.escape(plan['product_name'])}</b>\n"
        f"Plan: <b>{html.escape(plan['name'])}</b>\n"
        f"🛡 <b>Warranty:</b> {html.escape(plan.get('warranty') or '30 Days')}\n"
        f"💵 <b>Price:</b> ${float(plan['price']):.2f} USDT\n"
        "📦 <b>Available Now!</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 BUY NOW", callback_data=f"order_start_{plan_id}")]
    ])

    sent = 0
    for uid in subscribers:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await clear_subscribers_for_plan(plan_id)
    await log_admin_action(call.from_user.id, "Back in Stock Broadcast", f"Plan: {plan_id}, Sent: {sent}")
    await call.message.edit_text(f"✅ Dispatched back-in-stock alert to {sent} interested users.", parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_notify_all_"))
async def cb_dispatch_all_notif(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id FROM users")
            all_users = await cur.fetchall()

    text = (
        "🔥 <b>BACK IN STOCK!</b>\n\n"
        f"<b>{html.escape(plan['product_name'])}</b>\n"
        f"Plan: <b>{html.escape(plan['name'])}</b>\n"
        f"🛡 <b>Warranty:</b> {html.escape(plan.get('warranty') or '30 Days')}\n"
        f"💵 <b>Price:</b> ${float(plan['price']):.2f} USDT\n"
        "📦 <b>Available Now!</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 BUY NOW", callback_data=f"order_start_{plan_id}")]
    ])

    sent = 0
    for u in all_users:
        try:
            await bot.send_message(chat_id=u["id"], text=text, reply_markup=kb, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await clear_subscribers_for_plan(plan_id)
    await call.message.edit_text(f"✅ Dispatched notification to {sent} store users.", parse_mode="HTML")
    await call.answer()


# =============================================================================
# 18. ADMIN ORDER MANAGEMENT & APPROVAL / FULFILLMENT
# =============================================================================

@dp.callback_query(F.data == "admin_orders_menu")
async def cb_admin_orders_menu(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ Pending Payments", callback_data="adm_orders_list_PAYMENT_SUBMITTED")],
        [InlineKeyboardButton(text="💰 Paid / Processing", callback_data="adm_orders_list_PAID")],
        [InlineKeyboardButton(text="✅ Delivered", callback_data="adm_orders_list_DELIVERED")],
        [InlineKeyboardButton(text="📋 All Orders", callback_data="adm_orders_list_ALL")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text("🧾 <b>ORDER MANAGEMENT</b>\nSelect category to inspect:", reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_orders_list_"))
async def cb_adm_orders_list(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    status_filter = call.data.replace("adm_orders_list_", "")
    
    query = """
        SELECT o.id, o.amount, o.status, o.created_at, p.name AS product_name, pl.name AS plan_name, u.username
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN plans pl ON pl.id = o.plan_id
        JOIN users u ON u.id = o.user_id
        {where_clause}
        ORDER BY o.id DESC LIMIT 15;
    """
    where_clause = "" if status_filter == "ALL" else f"WHERE o.status = '{status_filter}'"
    
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query.format(where_clause=where_clause))
            orders = await cur.fetchall()

    if not orders:
        text = f"No orders found matching filter <code>{status_filter}</code>."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Back", callback_data="admin_orders_menu")]
        ])
    else:
        text = f"🧾 <b>Orders ({status_filter}):</b>\n\n"
        kb_rows = []
        for o in orders:
            btn_text = f"#{o['id']} - {o['product_name']} (${float(o['amount']):.2f}) [{o['status']}]"
            kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"adm_order_view_{o['id']}")])
        kb_rows.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_orders_menu")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_order_view_"))
async def cb_adm_order_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        await call.answer("Order not found.", show_alert=True)
        return

    text = (
        f"🧾 <b>ORDER #{order['id']} DETAILS</b>\n\n"
        f"<b>Customer:</b> @{html.escape(order['username'] or 'N/A')} (<code>{order['user_id']}</code>)\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(order.get('warranty') or '30 Days')}</b>\n"
        f"<b>Amount:</b> ${float(order['amount']):.2f} {order['currency']}\n"
        f"<b>Payment Method:</b> {order['payment_method'] or 'N/A'}\n"
        f"<b>Tx ID:</b> <code>{html.escape(order['tx_id'] or 'None')}</code>\n"
        f"<b>Status:</b> <code>{order['status']}</code>\n"
        f"<b>Created:</b> {order['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    if order["delivery_data"]:
        text += f"\n📦 <b>Delivered Credentials:</b>\n<code>{html.escape(order['delivery_data'])}</code>\n"

    kb_rows = []
    if order["status"] == "PAYMENT_SUBMITTED":
        kb_rows.append([
            InlineKeyboardButton(text="✅ APPROVE", callback_data=f"adm_pay_approve_{order_id}"),
            InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_pay_reject_{order_id}")
        ])
    elif order["status"] in ("PAID", "PROCESSING"):
        kb_rows.append([
            InlineKeyboardButton(text="📦 Auto-Fulfill from Stock", callback_data=f"adm_fulfill_auto_{order_id}"),
            InlineKeyboardButton(text="✍️ Manual Fulfill", callback_data=f"adm_fulfill_man_{order_id}")
        ])

    kb_rows.append([InlineKeyboardButton(text="◀️ Orders Menu", callback_data="admin_orders_menu")])

    if order.get("proof_file_id"):
        try:
            await call.message.delete()
            await bot.send_photo(
                chat_id=call.from_user.id,
                photo=order["proof_file_id"],
                caption=text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                parse_mode="HTML"
            )
            return
        except Exception:
            pass

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_pay_approve_"))
async def cb_adm_pay_approve(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        return

    if order["status"] not in ("PAYMENT_SUBMITTED", "PENDING_PAYMENT"):
        await call.answer(f"Status is {order['status']}.", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET status = 'PAID' WHERE id = %s", (order_id,))

    await log_admin_action(call.from_user.id, "Payment Approved", f"Order ID: {order_id}")

    try:
        await bot.send_message(
            chat_id=order["user_id"],
            text=f"✅ <b>Payment Confirmed!</b>\n\nYour payment for Order <code>#{order_id}</code> is approved. Processing fulfillment...",
            parse_mode="HTML"
        )
    except Exception:
        pass

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Auto-Fulfill from Stock", callback_data=f"adm_fulfill_auto_{order_id}")],
        [InlineKeyboardButton(text="✍️ Manual Fulfill", callback_data=f"adm_fulfill_man_{order_id}")],
        [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
    ])
    await call.message.edit_text(
        f"✅ Payment for Order <code>#{order_id}</code> APPROVED.\nChoose fulfillment mode:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_pay_reject_"))
async def cb_adm_pay_reject(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET status = 'PAYMENT_REJECTED' WHERE id = %s", (order_id,))

    await log_admin_action(call.from_user.id, "Payment Rejected", f"Order ID: {order_id}")

    try:
        support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
        await bot.send_message(
            chat_id=order["user_id"],
            text=f"❌ <b>Payment Rejected</b>\n\nCould not verify payment for Order <code>#{order_id}</code>. Please contact @{support_user}.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await call.message.edit_text(f"❌ Payment for Order #{order_id} marked REJECTED.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
    ]))
    await call.answer()

@dp.callback_query(F.data.startswith("adm_fulfill_auto_"))
async def cb_adm_fulfill_auto(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        return

    success, content = await fulfill_single_order(order_id, admin_id=call.from_user.id)
    if not success:
        await call.answer(content, show_alert=True)
        return

    delivery_msg = (
        "✅ <b>ORDER DELIVERED</b>\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(order.get('warranty') or '30 Days')}</b>\n\n"
        "📦 <b>Delivery Information:</b>\n"
        f"<code>{html.escape(content)}</code>\n\n"
        "Thank you for shopping with ISell Store!"
    )
    try:
        await bot.send_message(chat_id=order["user_id"], text=delivery_msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to send delivery to user %s: %s", order["user_id"], e)

    await log_admin_action(call.from_user.id, "Order Fulfilled (Auto)", f"Order #{order_id}")
    await call.message.edit_text(
        f"✅ Order <code>#{order_id}</code> fulfilled and delivered to customer!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_fulfill_man_"))
async def cb_adm_fulfill_manual_prompt(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    await state.set_state(AdminFulfillFSM.waiting_for_text)
    await state.update_data(order_id=order_id)
    await call.message.edit_text(
        f"✍️ <b>MANUAL FULFILLMENT FOR ORDER #{order_id}</b>\n\n"
        "Enter the license key, account details, or instructions to send to the buyer:",
        parse_mode="HTML"
    )
    await call.answer()

@dp.message(AdminFulfillFSM.waiting_for_text)
async def handle_admin_fulfill_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    order_id = data["order_id"]
    delivery_text = message.text.strip()
    
    order = await get_order_by_id(order_id)
    if not order:
        await state.clear()
        return

    success, content = await fulfill_single_order(order_id, admin_id=message.from_user.id, manual_delivery_text=delivery_text)
    await state.clear()

    if not success:
        await message.answer(f"Failed to fulfill: {content}")
        return

    delivery_msg = (
        "✅ <b>ORDER DELIVERED</b>\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Warranty:</b> 🛡 <b>{html.escape(order.get('warranty') or '30 Days')}</b>\n\n"
        "📦 <b>Delivery Information:</b>\n"
        f"<code>{html.escape(content)}</code>\n\n"
        "Thank you for shopping with ISell Store!"
    )
    try:
        await bot.send_message(chat_id=order["user_id"], text=delivery_msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to send manual delivery: %s", e)

    await log_admin_action(message.from_user.id, "Order Fulfilled (Manual)", f"Order #{order_id}")
    await message.answer(
        f"✅ Order <code>#{order_id}</code> fulfilled and sent to user.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
        ]),
        parse_mode="HTML"
    )


# =============================================================================
# 19. BROADCAST SYSTEM
# =============================================================================

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminBroadcastFSM.waiting_for_content)
    await message.answer("📢 <b>New Broadcast</b>\nSend the message (text, formatting, or photo) to broadcast:", parse_mode="HTML")

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminBroadcastFSM.waiting_for_content)
    await call.message.edit_text("📢 <b>BROADCAST DISPATCHER</b>\nEnter or forward the message you want to send to all users:", parse_mode="HTML")
    await call.answer()

@dp.message(AdminBroadcastFSM.waiting_for_content)
async def handle_broadcast_content(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    content_text = message.text or message.caption or ""
    photo_id = message.photo[-1].file_id if message.photo else None

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) AS cnt FROM users")
            total = (await cur.fetchone())["cnt"]

    await state.update_data(text=content_text, photo_id=photo_id, total=total)
    await state.set_state(AdminBroadcastFSM.confirm_send)

    preview_header = f"📢 <b>BROADCAST PREVIEW</b>\nRecipients: <b>{total} users</b>\n━━━━━━━━━━━━━━━━\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ SEND NOW", callback_data="adm_bcast_confirm"),
         InlineKeyboardButton(text="❌ CANCEL", callback_data="admin_dashboard")]
    ])

    if photo_id:
        await message.answer_photo(photo=photo_id, caption=preview_header + content_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(preview_header + content_text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(AdminBroadcastFSM.confirm_send, F.data == "adm_bcast_confirm")
async def cb_admin_bcast_confirm(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return

    data = await state.get_data()
    text = data.get("text", "")
    photo_id = data.get("photo_id")
    await state.clear()

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id FROM users")
            users = await cur.fetchall()

    status_msg = await call.message.answer("🚀 Broadcasting in progress...")
    sent, blocked, failed = 0, 0, 0

    for idx, u in enumerate(users):
        uid = u["id"]
        try:
            if photo_id:
                await bot.send_photo(chat_id=uid, photo=photo_id, caption=text, parse_mode="HTML")
            else:
                await bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            blocked += 1
        except Exception:
            failed += 1

        await asyncio.sleep(0.04)

    await log_admin_action(call.from_user.id, "Broadcast Sent", f"Delivered: {sent}, Blocked: {blocked}")
    await status_msg.edit_text(
        f"✅ <b>Broadcast Completed!</b>\n\n"
        f"• Delivered: <b>{sent}</b>\n"
        f"• Blocked: <b>{blocked}</b>\n"
        f"• Errors: <b>{failed}</b>",
        parse_mode="HTML"
    )
    await call.answer()


# =============================================================================
# 20. ADMIN SETTINGS, LOGS & USERS
# =============================================================================

@dp.callback_query(F.data == "admin_settings")
async def cb_admin_settings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return

    store_name = await get_setting("store_name", DEFAULT_STORE_NAME)
    binance_id = await get_setting("binance_id", DEFAULT_BINANCE_ID)
    usdt_addr = await get_setting("usdt_bep20", DEFAULT_USDT_ADDR)
    support_u = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)

    text = (
        "⚙️ <b>STORE CONFIGURATION</b>\n\n"
        f"<b>Store Title:</b> {html.escape(store_name)}\n"
        f"<b>Support Username:</b> @{support_u}\n"
        f"<b>Binance Pay ID:</b> <code>{binance_id}</code>\n"
        f"<b>USDT BEP-20 Address:</b>\n<code>{usdt_addr}</code>\n\n"
        "Select a parameter to modify:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Store Name", callback_data="adm_set_store_name"),
         InlineKeyboardButton(text="✏️ Support Username", callback_data="adm_set_support_username")],
        [InlineKeyboardButton(text="✏️ Binance ID", callback_data="adm_set_binance_id"),
         InlineKeyboardButton(text="✏️ USDT Address", callback_data="adm_set_usdt_bep20")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_set_"))
async def cb_admin_set_prompt(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    field = call.data.replace("adm_set_", "")
    await state.set_state(AdminSettingsFSM.waiting_for_value)
    await state.update_data(setting_key=field)
    await call.message.edit_text(f"Enter the new value for <code>{field}</code>:", parse_mode="HTML")
    await call.answer()

@dp.message(AdminSettingsFSM.waiting_for_value)
async def handle_admin_set_val(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data["setting_key"]
    val = message.text.strip().replace("@", "")
    await set_setting(key, val)
    await log_admin_action(message.from_user.id, "Setting Updated", f"{key} = {val}")
    await state.clear()
    await message.answer(f"✅ Setting <code>{key}</code> updated successfully.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Settings Menu", callback_data="admin_settings")]
    ]), parse_mode="HTML")

@dp.callback_query(F.data == "admin_logs")
async def cb_admin_logs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 15")
            logs = await cur.fetchall()

    if not logs:
        text = "📝 No admin logs recorded."
    else:
        text = "📝 <b>AUDIT LOGS (Last 15):</b>\n\n"
        for l in logs:
            text += f"• <code>{l['created_at'].strftime('%m-%d %H:%M')}</code> | <b>{html.escape(l['action'])}</b>: {html.escape(l['details'] or '')}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_logs"),
         InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, username, first_name, wallet_balance, created_at FROM users ORDER BY created_at DESC LIMIT 15")
            users = await cur.fetchall()

    text = "👥 <b>RECENT USERS:</b>\n\n"
    for u in users:
        wb = float(u["wallet_balance"]) if u["wallet_balance"] else 0.0
        text += f"• <code>{u['id']}</code> - @{html.escape(u['username'] or 'N/A')} | Bal: <b>${wb:.2f}</b>\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "ignore")
async def cb_ignore(call: CallbackQuery):
    await call.answer()


# =============================================================================
# 21. RENDER HEALTH CHECK WEB SERVER & MAIN ENTRY
# =============================================================================

async def handle_health_check(request):
    """Health check endpoint required when deployed as a Web Service on Render."""
    return web.Response(text="ISell Store Bot is running healthy 🟢")

async def start_web_server():
    """Binds to PORT environment variable provided by Render."""
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/health", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", RENDER_PORT)
    await site.start()
    logger.info("Render keep-alive web server listening on 0.0.0.0:%s", RENDER_PORT)

async def main():
    logger.info("Starting ISell Store Bot...")
    await init_db()

    # Start optional web server for Render Free Web Service
    try:
        await start_web_server()
    except Exception as e:
        logger.warning("Web server start skipped or error: %s (Standard polling continues)", e)

    # Register Bot Menu Commands
    customer_commands = [
        BotCommand(command="start", description="Open store"),
        BotCommand(command="store", description="Browse digital catalog"),
        BotCommand(command="wallet", description="Wallet balance & deposits"),
        BotCommand(command="orders", description="View your purchases"),
        BotCommand(command="profile", description="Your profile & stats"),
        BotCommand(command="help", description="Guide & support")
    ]
    await bot.set_my_commands(customer_commands)
    await bot.delete_webhook(drop_pending_updates=True)
    
    logger.info("Bot is polling for updates...")
    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            db_pool.close()
            await db_pool.wait_closed()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("ISell Store stopped.")

    # =============================================================================

@dp.callback_query(F.data.startswith("prod_view_"))
async def cb_prod_view(call: CallbackQuery):
    product_id = int(call.data.split("_")[2])
    product = await get_product_by_id(product_id)
    if not product:
        await call.answer("Product no longer exists.", show_alert=True)
        return

    plans = await get_plans_by_product_id(product_id, active_only=True)
    emoji = product.get("emoji") or "📦"
    text = f"{emoji} <b>{html.escape(product['name']).upper()}</b>\n"
    if product.get("description"):
        text += f"<i>{html.escape(product['description'])}</i>\n\n"
    else:
        text += "\n"

    if not plans:
        text += "❌ <b>Currently Sold Out</b>\nNo active tiers/plans have been configured for this product yet."
    else:
        text += "Select a plan below to proceed with order:"

    await call.message.edit_text(text, reply_markup=get_plans_kb(product_id, plans), parse_mode="HTML")
    await call.answer()


# =============================================================================
# 8. STOCK NOTIFICATIONS ("NOTIFY ME")
# =============================================================================

@dp.callback_query(F.data.startswith("notify_req_"))
async def cb_notify_request(call: CallbackQuery):
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)
    if not plan:
        await call.answer("Plan not found.", show_alert=True)
        return

    await subscribe_stock_notification(call.from_user.id, plan_id)
    await call.answer(
        f"🔔 You will be alerted immediately when {plan['name']} is back in stock!",
        show_alert=True
    )


# =============================================================================
# 9. CUSTOMER PURCHASE & PAYMENT WORKFLOW
# =============================================================================

@dp.callback_query(F.data.startswith("order_start_"))
async def cb_order_start(call: CallbackQuery):
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)
    if not plan or plan["available_count"] <= 0:
        await call.answer("Sorry, this plan is now sold out!", show_alert=True)
        return

    order_id = await create_order(call.from_user.id, plan_id)
    if not order_id:
        await call.answer("Failed to initiate order. Please try again.", show_alert=True)
        return

    text = (
        "🛒 <b>ORDER CONFIRMATION</b>\n\n"
        f"<b>Product:</b> {html.escape(plan['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(plan['name'])}\n"
        f"<b>Duration:</b> {plan['duration_days']} Days\n"
        f"<b>Total Price:</b> ${float(plan['price']):.2f} USDT\n\n"
        "Click <b>Confirm Order</b> to select your manual payment method."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm Order", callback_data=f"order_confirm_{order_id}")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data=f"order_cancel_{order_id}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("order_confirm_"))
async def cb_order_confirm(call: CallbackQuery):
    order_id = int(call.data.split("_")[2])
    order = await get_order_by_id(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Order not found or unauthorized.", show_alert=True)
        return

    text = (
        f"💳 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"Order: <code>#{order['id']}</code>\n"
        f"Amount: <b>${float(order['amount']):.2f} USDT</b>\n\n"
        "Choose how you would like to transfer funds:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟡 Binance ID (Internal Pay)", callback_data=f"order_pay_binance_{order_id}")],
        [InlineKeyboardButton(text="₮ USDT (BEP-20)", callback_data=f"order_pay_usdt_{order_id}")],
        [InlineKeyboardButton(text="❌ Cancel Order", callback_data=f"order_cancel_{order_id}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("order_pay_"))
async def cb_order_pay_method(call: CallbackQuery):
    parts = call.data.split("_")
    method = parts[2]
    order_id = int(parts[3])

    order = await get_order_by_id(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Invalid order.", show_alert=True)
        return

    # Record chosen method
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET payment_method = %s WHERE id = %s", (method.upper(), order_id))

    binance_id = await get_setting("binance_id", DEFAULT_BINANCE_ID)
    usdt_addr = await get_setting("usdt_bep20", DEFAULT_USDT_ADDR)

    if method == "binance":
        instr = (
            "🟡 <b>BINANCE TRANSFER DETAILS</b>\n\n"
            f"<b>Payee Binance ID:</b> <code>{binance_id}</code>\n"
            f"<b>Amount to Send:</b> <code>{float(order['amount']):.2f}</code> USDT\n\n"
            "<i>Instructions:</i>\n"
            "1. Open Binance App → Pay / Send\n"
            "2. Enter Binance ID above and exact amount.\n"
            "3. Complete transfer and copy Transaction ID / take screenshot.\n"
            "4. Click <b>I Have Paid</b> below."
        )
    else:
        instr = (
            "₮ <b>USDT BEP-20 TRANSFER DETAILS</b>\n\n"
            f"<b>Network:</b> BSC (BNB Smart Chain / BEP20)\n"
            f"<b>Deposit Address:</b>\n<code>{usdt_addr}</code>\n\n"
            f"<b>Exact Amount:</b> <code>{float(order['amount']):.2f}</code> USDT\n\n"
            "<i>Instructions:</i>\n"
            "1. Send USDT via BEP-20 network to the address above.\n"
            "2. Note down your TxHash / take screenshot.\n"
            "3. Click <b>I Have Paid</b> below."
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I Have Paid", callback_data=f"order_proof_{order_id}")],
        [InlineKeyboardButton(text="◀️ Back", callback_data=f"order_confirm_{order_id}")]
    ])
    await call.message.edit_text(instr, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("order_proof_"))
async def cb_order_proof_prompt(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split("_")[2])
    order = await get_order_by_id(order_id)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Order not found.", show_alert=True)
        return

    await state.set_state(CustomerPurchaseFSM.waiting_for_payment_proof)
    await state.update_data(order_id=order_id)

    prompt = (
        f"📝 <b>SUBMIT PAYMENT PROOF FOR ORDER #{order_id}</b>\n\n"
        "Please send one of the following now:\n"
        "1. <b>Transaction ID / Hash</b> as a text message, OR\n"
        "2. <b>Payment Screenshot</b> photo with caption, OR\n"
        "3. Both."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data=f"order_cancel_{order_id}")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(CustomerPurchaseFSM.waiting_for_payment_proof)
async def handle_payment_proof_submission(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    if not order_id:
        await state.clear()
        return

    tx_id = ""
    photo_file_id = ""

    if message.photo:
        photo_file_id = message.photo[-1].file_id
        tx_id = message.caption or "Attached in screenshot"
    elif message.text:
        tx_id = message.text.strip()
    else:
        await message.answer("Please send a valid text Transaction ID or screenshot.")
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE orders SET status = 'PAYMENT_SUBMITTED', tx_id = %s, proof_file_id = %s WHERE id = %s",
                (tx_id, photo_file_id, order_id)
            )

    await state.clear()

    order = await get_order_by_id(order_id)
    await message.answer(
        f"✅ <b>Payment Submitted!</b>\n\n"
        f"Your proof for Order <code>#{order_id}</code> is received and pending admin approval.\n"
        "You will be notified right here once approved and delivered.",
        reply_markup=get_customer_home_kb(message.from_user.id),
        parse_mode="HTML"
    )

    # Dispatch review notice to Admin (Telegram ID 7695407294)
    admin_notif_text = (
        "━━━━━━━━━━━━━━━━\n"
        "💰 <b>NEW PAYMENT SUBMITTED</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Customer:</b> @{html.escape(order['username'] or 'unknown')} (<code>{order['user_id']}</code>)\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Amount:</b> ${float(order['amount']):.2f} {order['currency']}\n"
        f"<b>Method:</b> {order['payment_method']}\n"
        f"<b>Tx ID / Note:</b> <code>{html.escape(order['tx_id'] or 'None')}</code>\n"
    )
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ APPROVE", callback_data=f"adm_pay_approve_{order_id}"),
         InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_pay_reject_{order_id}")],
        [InlineKeyboardButton(text="🔎 VIEW ORDER", callback_data=f"adm_order_view_{order_id}")]
    ])

    try:
        if order["proof_file_id"]:
            await bot.send_photo(
                chat_id=ADMIN_ID,
                photo=order["proof_file_id"],
                caption=admin_notif_text,
                reply_markup=admin_kb,
                parse_mode="HTML"
            )
        else:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_notif_text,
                reply_markup=admin_kb,
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error("Failed to notify admin of payment: %s", e)

@dp.callback_query(F.data.startswith("order_cancel_"))
async def cb_order_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    order_id = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE orders SET status = 'CANCELLED' WHERE id = %s AND user_id = %s AND status = 'PENDING_PAYMENT'",
                (order_id, call.from_user.id)
            )
    await call.message.edit_text("❌ Order cancelled.", reply_markup=get_customer_home_kb(call.from_user.id))
    await call.answer()


# =============================================================================
# 10. CUSTOMER: SEARCH, PROFILE & ORDERS
# =============================================================================

@dp.callback_query(F.data == "cust_search")
async def cb_cust_search(call: CallbackQuery, state: FSMContext):
    await state.set_state(CustomerSearchFSM.waiting_for_query)
    await call.message.edit_text("🔎 <b>Product Search</b>\n\nEnter product name or keyword:", parse_mode="HTML")
    await call.answer()

@dp.message(CustomerSearchFSM.waiting_for_query)
async def handle_customer_search_query(message: Message, state: FSMContext):
    await state.clear()
    query = message.text.strip().lower()
    all_products = await get_products_with_stock_info(active_only=True)
    matches = [p for p in all_products if query in p["name"].lower()]

    if not matches:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Browse Catalog", callback_data="cust_store_p0"),
             InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
        ])
        await message.answer(f"❌ No matching products found for '<b>{html.escape(query)}</b>'.", reply_markup=kb, parse_mode="HTML")
        return

    kb = get_products_grid_kb(matches, page=0, per_page=9)
    await message.answer(f"🔎 <b>Search Results for:</b> <i>{html.escape(query)}</i>", reply_markup=kb, parse_mode="HTML")

async def show_user_orders(user_id: int, target: Any):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT o.id, o.amount, o.status, o.created_at, p.name AS product_name, pl.name AS plan_name
                FROM orders o
                JOIN products p ON p.id = o.product_id
                JOIN plans pl ON pl.id = o.plan_id
                WHERE o.user_id = %s
                ORDER BY o.id DESC LIMIT 10;
            """, (user_id,))
            orders = await cur.fetchall()

    if not orders:
        text = "📦 <b>My Orders</b>\n\nYou haven't placed any orders yet."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍 Visit Store", callback_data="cust_store_p0")]
        ])
    else:
        text = "📦 <b>Your Recent Orders:</b>\n\n"
        for o in orders:
            status_emoji = "⏳" if "PAYMENT" in o["status"] else "✅" if o["status"] == "DELIVERED" else "⚙️"
            text += (
                f"{status_emoji} <b>Order #{o['id']}</b> — {html.escape(o['product_name'])} ({html.escape(o['plan_name'])})\n"
                f"Status: <code>{o['status']}</code> | ${float(o['amount']):.2f}\n"
                f"Date: {o['created_at'].strftime('%Y-%m-%d %H:%M')}\n\n"
            )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="cust_my_orders"),
             InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
        ])

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_my_orders")
async def cb_cust_my_orders(call: CallbackQuery):
    await show_user_orders(call.from_user.id, target=call)
    await call.answer()

async def show_user_profile(user_id: int, target: Any):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = await cur.fetchone()
            await cur.execute("SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0) AS total_spent FROM orders WHERE user_id = %s AND status = 'DELIVERED'", (user_id,))
            stats = await cur.fetchone()

    total_orders = stats["cnt"] if stats else 0
    total_spent = stats["total_spent"] if stats else 0

    text = (
        "👤 <b>USER PROFILE</b>\n\n"
        f"<b>Telegram ID:</b> <code>{user_id}</code>\n"
        f"<b>Username:</b> @{user['username'] if user and user['username'] else 'N/A'}\n"
        f"<b>Delivered Orders:</b> {total_orders}\n"
        f"<b>Total Volume:</b> ${float(total_spent):.2f} USDT\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 My Orders", callback_data="cust_my_orders"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "cust_profile")
async def cb_cust_profile(call: CallbackQuery):
    await show_user_profile(call.from_user.id, target=call)
    await call.answer()

@dp.callback_query(F.data == "cust_support")
async def cb_cust_support(call: CallbackQuery):
    support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
    text = (
        "📞 <b>Customer Support Desk</b>\n\n"
        "Our team is ready to assist you with order verification, custom subscriptions, or inquiries.\n\n"
        f"Direct Telegram: @{support_user}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Message Support", url=f"https://t.me/{support_user}")],
        [InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "cust_notifs")
async def cb_cust_notifs(call: CallbackQuery):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT p.name AS product_name, pl.name AS plan_name
                FROM stock_notifications sn
                JOIN plans pl ON pl.id = sn.plan_id
                JOIN products p ON p.id = pl.product_id
                WHERE sn.user_id = %s
            """, (call.from_user.id,))
            subs = await cur.fetchall()

    if not subs:
        text = "🔔 <b>Stock Alerts</b>\n\nYou currently have no active back-in-stock alerts."
    else:
        text = "🔔 <b>Your Active Back-In-Stock Subscriptions:</b>\n\n"
        for s in subs:
            text += f"• {html.escape(s['product_name'])} — {html.escape(s['plan_name'])}\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Store", callback_data="cust_store_p0"),
         InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "cust_help")
async def cb_cust_help(call: CallbackQuery):
    support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
    text = (
        "ℹ️ <b>ISell Store Help & Guides</b>\n\n"
        "• <b>How to Buy:</b> Select a product, pick an active plan, and confirm your order.\n"
        "• <b>Payment:</b> Send payment via Binance ID or USDT BEP-20, then submit your transaction ID.\n"
        "• <b>Fulfillment:</b> Once payment is confirmed by admin, your credentials will arrive here automatically.\n"
        "• <b>Alerts:</b> If an item is sold out, click 🔔 Notify Me to be alerted the moment it's restocked.\n\n"
        f"Need personal assistance? Contact @{support_user}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Home", callback_data="cust_home")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =============================================================================
# 11. ADMIN PANEL: DASHBOARD & STATS
# =============================================================================

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return  # Silently ignore non-admins
    await state.clear()
    await message.answer("⚙️ <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_dashboard_kb(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_dashboard")
async def cb_admin_dashboard(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Unauthorized", show_alert=True)
        return
    await state.clear()
    await call.message.edit_text("⚙️ <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_dashboard_kb(), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Unauthorized", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) AS total_users FROM users")
            total_users = (await cur.fetchone())["total_users"]

            await cur.execute("SELECT COUNT(*) AS total_orders FROM orders")
            total_orders = (await cur.fetchone())["total_orders"]

            await cur.execute("SELECT COUNT(*) AS pending FROM orders WHERE status = 'PAYMENT_SUBMITTED'")
            pending = (await cur.fetchone())["pending"]

            await cur.execute("SELECT COUNT(*) AS delivered FROM orders WHERE status = 'DELIVERED'")
            completed = (await cur.fetchone())["delivered"]

            await cur.execute("SELECT COALESCE(SUM(amount), 0) AS revenue FROM orders WHERE status = 'DELIVERED'")
            revenue = (await cur.fetchone())["revenue"]

            await cur.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN i.status = 'AVAILABLE' THEN pl.id END) AS available_plans,
                    COUNT(DISTINCT CASE WHEN i.id IS NULL OR i.status != 'AVAILABLE' THEN pl.id END) AS sold_out_plans
                FROM plans pl
                LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
                WHERE pl.is_active = 1;
            """)
            plan_stats = await cur.fetchone()

    stats_text = (
        "📊 <b>DASHBOARD STATISTICS</b>\n\n"
        f"👥 <b>Users:</b> {total_users}\n"
        f"🧾 <b>Orders:</b> {total_orders}\n"
        f"⏳ <b>Pending Payments:</b> {pending}\n"
        f"✅ <b>Completed:</b> {completed}\n"
        f"💰 <b>Revenue:</b> ${float(revenue):,.2f} USDT\n\n"
        f"📦 <b>Available Plans:</b> {plan_stats['available_plans'] or 0}\n"
        f"❌ <b>Sold Out Plans:</b> {plan_stats['sold_out_plans'] or 0}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_stats")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(stats_text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =============================================================================
# 12. ADMIN PRODUCT MANAGEMENT
# =============================================================================

@dp.callback_query(F.data.startswith("admin_prods_p"))
async def cb_admin_products(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Unauthorized", show_alert=True)
        return

    page = int(call.data.replace("admin_prods_p", ""))
    products = await get_products_with_stock_info(active_only=False)
    per_page = 8
    total_pages = max(1, math.ceil(len(products) / per_page))
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    sliced = products[start:start + per_page]

    kb_rows = []
    for p in sliced:
        status_flag = "🟢" if p["is_active"] else "⏸"
        stock_flag = "📦" if p["available_stock_count"] > 0 else "❌"
        btn_text = f"{status_flag}{stock_flag} {p['emoji']} {p['name']}"
        kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"adm_pdetail_{p['id']}")])

    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_prods_p{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_prods_p{page+1}"))
    kb_rows.append(nav)

    kb_rows.append([InlineKeyboardButton(text="➕ Add Product", callback_data="adm_padd")])
    kb_rows.append([InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")])

    await call.message.edit_text("🛍 <b>PRODUCT CATALOG MANAGEMENT</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "adm_padd")
async def cb_adm_padd(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminProductFSM.waiting_for_name)
    await call.message.edit_text("➕ <b>New Product</b>\nEnter the product title (e.g. <code>ChatGPT</code>):", parse_mode="HTML")
    await call.answer()

@dp.message(AdminProductFSM.waiting_for_name)
async def handle_padd_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminProductFSM.waiting_for_emoji)
    await message.answer("Enter an emoji/icon for this product (e.g. 🤖):")

@dp.message(AdminProductFSM.waiting_for_emoji)
async def handle_padd_emoji(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(emoji=message.text.strip())
    await state.set_state(AdminProductFSM.waiting_for_description)
    await message.answer("Enter product description / overview:")

@dp.message(AdminProductFSM.waiting_for_description)
async def handle_padd_desc(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    desc = message.text.strip()
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO products (name, emoji, description, is_active) VALUES (%s, %s, %s, 1)",
                (data["name"], data["emoji"], desc)
            )
            new_id = cur.lastrowid

    await log_admin_action(message.from_user.id, "Product Created", f"ID: {new_id}, Name: {data['name']}")
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Plan Now", callback_data=f"adm_plan_add_{new_id}")],
        [InlineKeyboardButton(text="🛍 Product List", callback_data="admin_prods_p0")]
    ])
    await message.answer(f"✅ Product <b>{html.escape(data['name'])}</b> created with <b>NO STOCK</b>.", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("adm_pdetail_"))
async def cb_adm_pdetail(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    p = await get_product_by_id(pid)
    plans = await get_plans_by_product_id(pid, active_only=False)

    text = (
        f"{p['emoji']} <b>{html.escape(p['name'])}</b> (ID: #{p['id']})\n"
        f"<b>Status:</b> {'🟢 Active' if p['is_active'] else '⏸ Disabled'}\n"
        f"<b>Description:</b> {html.escape(p['description'] or 'None')}\n\n"
        f"<b>Configured Plans:</b> {len(plans)}\n"
    )
    for pl in plans:
        text += f"• {pl['name']}: ${float(pl['price']):.2f} (Stock: {pl['available_count']})\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Plan", callback_data=f"adm_plan_add_{pid}"),
         InlineKeyboardButton(text="📋 Manage Plans", callback_data=f"adm_plans_list_{pid}")],
        [InlineKeyboardButton(text="🔄 Toggle Active", callback_data=f"adm_ptoggle_{pid}"),
         InlineKeyboardButton(text="🗑 Delete Product", callback_data=f"adm_pdel_{pid}")],
        [InlineKeyboardButton(text="◀️ Products", callback_data="admin_prods_p0")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_ptoggle_"))
async def cb_adm_ptoggle(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE products SET is_active = NOT is_active WHERE id = %s", (pid,))
    await cb_adm_pdetail(call)

@dp.callback_query(F.data.startswith("adm_pdel_"))
async def cb_adm_pdel(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[2])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM products WHERE id = %s", (pid,))
    await log_admin_action(call.from_user.id, "Product Deleted", f"Product ID: {pid}")
    await call.answer("Product deleted successfully.", show_alert=True)
    call.data = "admin_prods_p0"
    await cb_admin_products(call)


# =============================================================================
# 13. ADMIN PLAN MANAGEMENT
# =============================================================================

@dp.callback_query(F.data.startswith("adm_plans_list_"))
async def cb_adm_plans_list(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[3])
    p = await get_product_by_id(pid)
    plans = await get_plans_by_product_id(pid, active_only=False)

    rows = []
    for pl in plans:
        st = "🟢" if pl["is_active"] else "⏸"
        rows.append([InlineKeyboardButton(
            text=f"{st} {pl['name']} - ${float(pl['price']):.2f} (📦 {pl['available_count']})",
            callback_data=f"adm_plandetail_{pl['id']}"
        )])

    rows.append([InlineKeyboardButton(text="➕ Add New Plan", callback_data=f"adm_plan_add_{pid}")])
    rows.append([InlineKeyboardButton(text="◀️ Product Detail", callback_data=f"adm_pdetail_{pid}")])

    await call.message.edit_text(
        f"📋 <b>PLANS FOR {html.escape(p['name'].upper())}</b>\nSelect a plan to edit, adjust price, or manage stock:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_plan_add_"))
async def cb_adm_plan_add(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    pid = int(call.data.split("_")[3])
    await state.set_state(AdminPlanFSM.waiting_for_name)
    await state.update_data(product_id=pid)
    await call.message.edit_text("Enter Plan Name (e.g. <code>GPT TEAM 1 Month</code>):", parse_mode="HTML")
    await call.answer()

@dp.message(AdminPlanFSM.waiting_for_name)
async def handle_plan_add_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPlanFSM.waiting_for_price)
    await message.answer("Enter Price in USD/USDT (e.g. <code>15.00</code>):", parse_mode="HTML")

@dp.message(AdminPlanFSM.waiting_for_price)
async def handle_plan_add_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        price = float(message.text.strip().replace("$", ""))
    except ValueError:
        await message.answer("Invalid price format. Enter e.g. 15.00:")
        return
    await state.update_data(price=price)
    await state.set_state(AdminPlanFSM.waiting_for_duration)
    await message.answer("Enter duration in days (e.g. <code>30</code>):", parse_mode="HTML")

@dp.message(AdminPlanFSM.waiting_for_duration)
async def handle_plan_add_duration(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        duration = int(message.text.strip())
    except ValueError:
        await message.answer("Invalid duration. Enter whole number of days:")
        return

    data = await state.get_data()
    pid = data["product_id"]

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO plans (product_id, name, price, duration_days, is_active) VALUES (%s, %s, %s, %s, 1)",
                (pid, data["name"], data["price"], duration)
            )
            plan_id = cur.lastrowid

    await log_admin_action(message.from_user.id, "Plan Created", f"Plan ID: {plan_id}, Product: {pid}")
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Stock Now", callback_data=f"adm_stock_add_{plan_id}")],
        [InlineKeyboardButton(text="📋 Plans List", callback_data=f"adm_plans_list_{pid}")]
    ])
    await message.answer(
        f"✅ Plan <b>{html.escape(data['name'])}</b> created successfully.\n"
        "Stock is currently <b>0 (Sold Out)</b> until you add inventory items.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("adm_plandetail_"))
async def cb_adm_plandetail(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[2])
    plan = await get_plan_by_id(plan_id)

    text = (
        f"⚙️ <b>PLAN SETTINGS: {html.escape(plan['name'])}</b>\n"
        f"Product: {html.escape(plan['product_name'])}\n"
        f"Price: ${float(plan['price']):.2f}\n"
        f"Duration: {plan['duration_days']} Days\n"
        f"Active: {'🟢 Yes' if plan['is_active'] else '⏸ No'}\n"
        f"Available Stock: <b>{plan['available_count']}</b> units\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Stock", callback_data=f"adm_stock_add_{plan_id}"),
         InlineKeyboardButton(text="📦 View Inventory", callback_data=f"adm_stock_view_{plan_id}")],
        [InlineKeyboardButton(text="💰 Change Price", callback_data=f"adm_plan_chgprice_{plan_id}"),
         InlineKeyboardButton(text="🔄 Toggle Active", callback_data=f"adm_plan_toggle_{plan_id}")],
        [InlineKeyboardButton(text="🗑 Delete Plan", callback_data=f"adm_plan_del_{plan_id}")],
        [InlineKeyboardButton(text="◀️ Product", callback_data=f"adm_pdetail_{plan['product_id']}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_plan_toggle_"))
async def cb_adm_plan_toggle(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE plans SET is_active = NOT is_active WHERE id = %s", (plan_id,))
    call.data = f"adm_plandetail_{plan_id}"
    await cb_adm_plandetail(call)

@dp.callback_query(F.data.startswith("adm_plan_del_"))
async def cb_adm_plan_del(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM plans WHERE id = %s", (plan_id,))
    await log_admin_action(call.from_user.id, "Plan Deleted", f"Plan ID: {plan_id}")
    await call.answer("Plan deleted.", show_alert=True)
    call.data = f"adm_plans_list_{plan['product_id']}"
    await cb_adm_plans_list(call)


# =============================================================================
# 14. INVENTORY & STOCK RESTOCK MANAGEMENT
# =============================================================================

@dp.callback_query(F.data == "admin_inv_root")
async def cb_admin_inv_root(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    # List all plans with stock count
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT pl.id, pl.name, p.name AS prod_name,
                       COUNT(CASE WHEN i.status = 'AVAILABLE' THEN 1 END) AS available_count
                FROM plans pl
                JOIN products p ON p.id = pl.product_id
                LEFT JOIN inventory i ON i.plan_id = pl.id AND i.status = 'AVAILABLE'
                GROUP BY pl.id
                ORDER BY p.name ASC;
            """)
            rows = await cur.fetchall()

    kb_rows = []
    for r in rows:
        kb_rows.append([InlineKeyboardButton(
            text=f"{r['prod_name']} - {r['name']} (📦 {r['available_count']})",
            callback_data=f"adm_stock_add_{r['id']}"
        )])

    kb_rows.append([InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")])
    await call.message.edit_text("📦 <b>SELECT PLAN TO RESTOCK:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_stock_add_"))
async def cb_adm_stock_add(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)

    await state.set_state(AdminStockFSM.waiting_for_items)
    await state.update_data(plan_id=plan_id, old_stock=plan["available_count"])

    prompt = (
        f"➕ <b>RESTOCK INVENTORY: {html.escape(plan['name'])}</b>\n"
        f"Current Stock: <b>{plan['available_count']}</b>\n\n"
        "Send your inventory items below, <b>one item per line</b>.\n"
        "Example:\n"
        "<code>user1:pass1\nuser2:pass2\nKEY-XXXX-YYYY</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data=f"adm_plandetail_{plan_id}")]
    ])
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.message(AdminStockFSM.waiting_for_items)
async def handle_admin_stock_items(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    plan_id = data["plan_id"]
    old_stock = data.get("old_stock", 0)

    lines = [line.strip() for line in message.text.split("\n") if line.strip()]
    if not lines:
        await message.answer("No valid items detected. Please send lines of credentials/codes.")
        return

    added = await add_stock_items(plan_id, lines, item_type="CODE")
    new_stock = old_stock + added
    plan = await get_plan_by_id(plan_id)

    await log_admin_action(message.from_user.id, "Stock Added", f"Plan ID: {plan_id}, Count: {added}")
    await state.clear()

    # If it was previously sold out (old_stock == 0) and now restocked, offer notification trigger
    if old_stock == 0 and added > 0:
        subs = await get_subscribers_for_plan(plan_id)
        preview_text = (
            "🔥 <b>BACK IN STOCK PREVIEW</b>\n\n"
            f"<b>Product:</b> {html.escape(plan['product_name'])}\n"
            f"<b>Plan:</b> {html.escape(plan['name'])}\n"
            f"<b>Price:</b> ${float(plan['price']):.2f}\n"
            f"<b>New Stock:</b> {new_stock} Available\n\n"
            f"Interested subscribers waiting: <b>{len(subs)}</b>\n"
            "Would you like to dispatch back-in-stock notifications?"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📢 Notify Interested ({len(subs)})", callback_data=f"adm_notify_subs_{plan_id}")],
            [InlineKeyboardButton(text="📢 Notify All Users", callback_data=f"adm_notify_all_{plan_id}")],
            [InlineKeyboardButton(text="❌ Skip Notification", callback_data=f"adm_plandetail_{plan_id}")]
        ])
        await message.answer(preview_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(
            f"✅ Successfully added <b>{added}</b> inventory items.\nTotal stock: <b>{new_stock}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Plan Detail", callback_data=f"adm_plandetail_{plan_id}")]
            ]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("adm_stock_view_"))
async def cb_adm_stock_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, item_type, content, status FROM inventory WHERE plan_id = %s ORDER BY status ASC, id DESC LIMIT 20",
                (plan_id,)
            )
            items = await cur.fetchall()

    if not items:
        text = "📦 No inventory found for this plan."
    else:
        text = f"📦 <b>Recent Inventory Items (Plan #{plan_id}):</b>\n\n"
        for it in items:
            text += f"• [<code>{it['status']}</code>] {html.escape(it['content'][:40])}...\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add More Stock", callback_data=f"adm_stock_add_{plan_id}")],
        [InlineKeyboardButton(text="◀️ Back to Plan", callback_data=f"adm_plandetail_{plan_id}")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =============================================================================
# 15. RESTOCK NOTIFICATION DISPATCHER
# =============================================================================

@dp.callback_query(F.data.startswith("adm_notify_subs_"))
async def cb_dispatch_subs_notif(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)
    subscribers = await get_subscribers_for_plan(plan_id)

    text = (
        "🔥 <b>BACK IN STOCK!</b>\n\n"
        f"<b>{html.escape(plan['product_name'])}</b>\n"
        f"Plan: <b>{html.escape(plan['name'])}</b>\n"
        f"💵 <b>Price:</b> ${float(plan['price']):.2f} USDT\n"
        "📦 <b>Available Now!</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 BUY NOW", callback_data=f"order_start_{plan_id}")]
    ])

    sent = 0
    for uid in subscribers:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await clear_subscribers_for_plan(plan_id)
    await log_admin_action(call.from_user.id, "Back in Stock Broadcast", f"Plan ID: {plan_id}, Sent: {sent}")
    await call.message.edit_text(f"✅ Dispatched back-in-stock notification to {sent} interested users.", parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_notify_all_"))
async def cb_dispatch_all_notif(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    plan_id = int(call.data.split("_")[3])
    plan = await get_plan_by_id(plan_id)

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id FROM users")
            all_users = await cur.fetchall()

    text = (
        "🔥 <b>BACK IN STOCK!</b>\n\n"
        f"<b>{html.escape(plan['product_name'])}</b>\n"
        f"Plan: <b>{html.escape(plan['name'])}</b>\n"
        f"💵 <b>Price:</b> ${float(plan['price']):.2f} USDT\n"
        "📦 <b>Available Now!</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 BUY NOW", callback_data=f"order_start_{plan_id}")]
    ])

    sent = 0
    for u in all_users:
        try:
            await bot.send_message(chat_id=u["id"], text=text, reply_markup=kb, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    await clear_subscribers_for_plan(plan_id)
    await call.message.edit_text(f"✅ Dispatched notification to {sent} store users.", parse_mode="HTML")
    await call.answer()


# =============================================================================
# 16. ADMIN ORDER MANAGEMENT & APPROVAL / FULFILLMENT
# =============================================================================

@dp.callback_query(F.data == "admin_orders_menu")
async def cb_admin_orders_menu(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ Pending Payments", callback_data="adm_orders_list_PAYMENT_SUBMITTED")],
        [InlineKeyboardButton(text="💰 Paid / Processing", callback_data="adm_orders_list_PAID")],
        [InlineKeyboardButton(text="✅ Delivered", callback_data="adm_orders_list_DELIVERED")],
        [InlineKeyboardButton(text="📋 All Recent Orders", callback_data="adm_orders_list_ALL")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text("🧾 <b>ORDER MANAGEMENT</b>\nSelect category to inspect:", reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_orders_list_"))
async def cb_adm_orders_list(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    status_filter = call.data.replace("adm_orders_list_", "")
    
    query = """
        SELECT o.id, o.amount, o.status, o.created_at, p.name AS product_name, pl.name AS plan_name, u.username
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN plans pl ON pl.id = o.plan_id
        JOIN users u ON u.id = o.user_id
        {where_clause}
        ORDER BY o.id DESC LIMIT 15;
    """
    where_clause = "" if status_filter == "ALL" else f"WHERE o.status = '{status_filter}'"
    
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query.format(where_clause=where_clause))
            orders = await cur.fetchall()

    if not orders:
        text = f"No orders found matching filter <code>{status_filter}</code>."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Back", callback_data="admin_orders_menu")]
        ])
    else:
        text = f"🧾 <b>Orders ({status_filter}):</b>\n\n"
        kb_rows = []
        for o in orders:
            btn_text = f"#{o['id']} - {o['product_name']} (${float(o['amount']):.2f}) [{o['status']}]"
            kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"adm_order_view_{o['id']}")])
        kb_rows.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_orders_menu")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_order_view_"))
async def cb_adm_order_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        await call.answer("Order not found.", show_alert=True)
        return

    text = (
        f"🧾 <b>ORDER #{order['id']} DETAILS</b>\n\n"
        f"<b>Customer:</b> @{html.escape(order['username'] or 'N/A')} (<code>{order['user_id']}</code>)\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n"
        f"<b>Amount:</b> ${float(order['amount']):.2f} {order['currency']}\n"
        f"<b>Payment Method:</b> {order['payment_method'] or 'N/A'}\n"
        f"<b>Tx ID:</b> <code>{html.escape(order['tx_id'] or 'None')}</code>\n"
        f"<b>Status:</b> <code>{order['status']}</code>\n"
        f"<b>Created:</b> {order['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    if order["delivery_data"]:
        text += f"\n📦 <b>Delivered Credentials:</b>\n<code>{html.escape(order['delivery_data'])}</code>\n"

    kb_rows = []
    if order["status"] == "PAYMENT_SUBMITTED":
        kb_rows.append([
            InlineKeyboardButton(text="✅ APPROVE", callback_data=f"adm_pay_approve_{order_id}"),
            InlineKeyboardButton(text="❌ REJECT", callback_data=f"adm_pay_reject_{order_id}")
        ])
    elif order["status"] in ("PAID", "PROCESSING"):
        kb_rows.append([
            InlineKeyboardButton(text="📦 FULFILL AUTOMATICALLY", callback_data=f"adm_fulfill_auto_{order_id}"),
            InlineKeyboardButton(text="✍️ MANUAL FULFILL", callback_data=f"adm_fulfill_man_{order_id}")
        ])

    kb_rows.append([InlineKeyboardButton(text="◀️ Orders Menu", callback_data="admin_orders_menu")])

    if order.get("proof_file_id"):
        try:
            await call.message.delete()
            await bot.send_photo(
                chat_id=call.from_user.id,
                photo=order["proof_file_id"],
                caption=text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                parse_mode="HTML"
            )
            return
        except Exception:
            pass

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_pay_approve_"))
async def cb_adm_pay_approve(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        await call.answer("Order not found.", show_alert=True)
        return

    if order["status"] not in ("PAYMENT_SUBMITTED", "PENDING_PAYMENT"):
        await call.answer(f"Cannot approve. Current status is {order['status']}.", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET status = 'PAID' WHERE id = %s", (order_id,))

    await log_admin_action(call.from_user.id, "Payment Approved", f"Order ID: {order_id}")

    try:
        await bot.send_message(
            chat_id=order["user_id"],
            text=f"✅ <b>Payment Confirmed!</b>\n\nYour payment for Order <code>#{order_id}</code> has been approved. Processing fulfillment...",
            parse_mode="HTML"
        )
    except Exception:
        pass

    # Prompt admin to immediately fulfill
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Auto-Fulfill from Stock", callback_data=f"adm_fulfill_auto_{order_id}")],
        [InlineKeyboardButton(text="✍️ Enter Manual Details", callback_data=f"adm_fulfill_man_{order_id}")],
        [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
    ])
    await call.message.edit_text(
        f"✅ Payment for Order <code>#{order_id}</code> APPROVED.\nStatus is now <b>PAID</b>. Fulfill the order now:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_pay_reject_"))
async def cb_adm_pay_reject(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        return

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE orders SET status = 'PAYMENT_REJECTED' WHERE id = %s", (order_id,))

    await log_admin_action(call.from_user.id, "Payment Rejected", f"Order ID: {order_id}")

    try:
        support_user = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)
        await bot.send_message(
            chat_id=order["user_id"],
            text=(
                f"❌ <b>Payment Rejected</b>\n\n"
                f"We could not verify your payment for Order <code>#{order_id}</code>.\n"
                f"Please reach out to support @{support_user} with your proof."
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    await call.message.edit_text(f"❌ Payment for Order #{order_id} marked REJECTED.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
    ]))
    await call.answer()

@dp.callback_query(F.data.startswith("adm_fulfill_auto_"))
async def cb_adm_fulfill_auto(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    order = await get_order_by_id(order_id)
    if not order:
        await call.answer("Order not found.", show_alert=True)
        return

    success, content = await fulfill_single_order(order_id, admin_id=call.from_user.id)
    if not success:
        await call.answer(content, show_alert=True)
        return

    # Deliver to customer
    delivery_msg = (
        "✅ <b>ORDER DELIVERED</b>\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n\n"
        "📦 <b>Delivery Credentials:</b>\n"
        f"<code>{html.escape(content)}</code>\n\n"
        "Thank you for purchasing with ISell Store!"
    )
    try:
        await bot.send_message(chat_id=order["user_id"], text=delivery_msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to send delivery to user %s: %s", order["user_id"], e)

    await log_admin_action(call.from_user.id, "Order Fulfilled (Auto)", f"Order #{order_id}")
    await call.message.edit_text(
        f"✅ Order <code>#{order_id}</code> successfully fulfilled and delivered to customer!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("adm_fulfill_man_"))
async def cb_adm_fulfill_manual_prompt(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    order_id = int(call.data.split("_")[3])
    await state.set_state(AdminFulfillFSM.waiting_for_text)
    await state.update_data(order_id=order_id)
    await call.message.edit_text(
        f"✍️ <b>MANUAL FULFILLMENT FOR ORDER #{order_id}</b>\n\n"
        "Enter the license key, account details, or instructions to send to the buyer:",
        parse_mode="HTML"
    )
    await call.answer()

@dp.message(AdminFulfillFSM.waiting_for_text)
async def handle_admin_fulfill_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    order_id = data["order_id"]
    delivery_text = message.text.strip()
    
    order = await get_order_by_id(order_id)
    if not order:
        await state.clear()
        return

    success, content = await fulfill_single_order(order_id, admin_id=message.from_user.id, manual_delivery_text=delivery_text)
    await state.clear()

    if not success:
        await message.answer(f"Failed to fulfill: {content}")
        return

    delivery_msg = (
        "✅ <b>ORDER DELIVERED</b>\n\n"
        f"<b>Order:</b> <code>#{order['id']}</code>\n"
        f"<b>Product:</b> {html.escape(order['product_name'])}\n"
        f"<b>Plan:</b> {html.escape(order['plan_name'])}\n\n"
        "📦 <b>Delivery Information:</b>\n"
        f"<code>{html.escape(content)}</code>\n\n"
        "Thank you for shopping with ISell Store!"
    )
    try:
        await bot.send_message(chat_id=order["user_id"], text=delivery_msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Failed to send manual delivery: %s", e)

    await log_admin_action(message.from_user.id, "Order Fulfilled (Manual)", f"Order #{order_id}")
    await message.answer(
        f"✅ Order <code>#{order_id}</code> fulfilled and sent to user.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orders", callback_data="admin_orders_menu")]
        ]),
        parse_mode="HTML"
    )


# =============================================================================
# 17. BROADCAST SYSTEM
# =============================================================================

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminBroadcastFSM.waiting_for_content)
    await message.answer("📢 <b>New Broadcast</b>\nSend the message (text, formatting, or photo) you wish to broadcast:", parse_mode="HTML")

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminBroadcastFSM.waiting_for_content)
    await call.message.edit_text("📢 <b>BROADCAST DISPATCHER</b>\nEnter or forward the message you want to send to all users:", parse_mode="HTML")
    await call.answer()

@dp.message(AdminBroadcastFSM.waiting_for_content)
async def handle_broadcast_content(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    content_text = message.text or message.caption or ""
    photo_id = message.photo[-1].file_id if message.photo else None

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) AS cnt FROM users")
            total = (await cur.fetchone())["cnt"]

    await state.update_data(text=content_text, photo_id=photo_id, total=total)
    await state.set_state(AdminBroadcastFSM.confirm_send)

    preview_header = f"📢 <b>BROADCAST PREVIEW</b>\nRecipients: <b>{total} users</b>\n━━━━━━━━━━━━━━━━\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ SEND NOW", callback_data="adm_bcast_confirm"),
         InlineKeyboardButton(text="❌ CANCEL", callback_data="admin_dashboard")]
    ])

    if photo_id:
        await message.answer_photo(photo=photo_id, caption=preview_header + content_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(preview_header + content_text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(AdminBroadcastFSM.confirm_send, F.data == "adm_bcast_confirm")
async def cb_admin_bcast_confirm(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return

    data = await state.get_data()
    text = data.get("text", "")
    photo_id = data.get("photo_id")
    await state.clear()

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id FROM users")
            users = await cur.fetchall()

    status_msg = await call.message.answer("🚀 Broadcasting in progress...")
    sent, blocked, failed = 0, 0, 0

    for idx, u in enumerate(users):
        uid = u["id"]
        try:
            if photo_id:
                await bot.send_photo(chat_id=uid, photo=photo_id, caption=text, parse_mode="HTML")
            else:
                await bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            blocked += 1
        except Exception:
            failed += 1

        # Rate limiting (approx 25 msgs/sec)
        await asyncio.sleep(0.04)

        if idx % 50 == 0 and idx > 0:
            try:
                await status_msg.edit_text(f"🚀 Sent: {sent} | Blocked: {blocked} | Progress: {idx}/{len(users)}")
            except Exception:
                pass

    await log_admin_action(call.from_user.id, "Broadcast Sent", f"Delivered: {sent}, Blocked: {blocked}")
    await status_msg.edit_text(
        f"✅ <b>Broadcast Completed!</b>\n\n"
        f"• Delivered: <b>{sent}</b>\n"
        f"• Blocked / Deactivated: <b>{blocked}</b>\n"
        f"• Errors: <b>{failed}</b>",
        parse_mode="HTML"
    )
    await call.answer()


# =============================================================================
# 18. ADMIN SETTINGS, LOGS & USERS
# =============================================================================

@dp.callback_query(F.data == "admin_settings")
async def cb_admin_settings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return

    store_name = await get_setting("store_name", DEFAULT_STORE_NAME)
    binance_id = await get_setting("binance_id", DEFAULT_BINANCE_ID)
    usdt_addr = await get_setting("usdt_bep20", DEFAULT_USDT_ADDR)
    support_u = await get_setting("support_username", DEFAULT_SUPPORT_USERNAME)

    text = (
        "⚙️ <b>STORE CONFIGURATION</b>\n\n"
        f"<b>Store Title:</b> {html.escape(store_name)}\n"
        f"<b>Support Username:</b> @{support_u}\n"
        f"<b>Binance Pay ID:</b> <code>{binance_id}</code>\n"
        f"<b>USDT BEP-20 Address:</b>\n<code>{usdt_addr}</code>\n\n"
        "Select a parameter to modify:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Store Name", callback_data="adm_set_store_name"),
         InlineKeyboardButton(text="✏️ Support Username", callback_data="adm_set_support_username")],
        [InlineKeyboardButton(text="✏️ Binance ID", callback_data="adm_set_binance_id"),
         InlineKeyboardButton(text="✏️ USDT Address", callback_data="adm_set_usdt_bep20")],
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("adm_set_"))
async def cb_admin_set_prompt(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    field = call.data.replace("adm_set_", "")
    await state.set_state(AdminSettingsFSM.waiting_for_value)
    await state.update_data(setting_key=field)
    await call.message.edit_text(f"Enter the new value for <code>{field}</code>:", parse_mode="HTML")
    await call.answer()

@dp.message(AdminSettingsFSM.waiting_for_value)
async def handle_admin_set_val(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data["setting_key"]
    val = message.text.strip().replace("@", "")
    await set_setting(key, val)
    await log_admin_action(message.from_user.id, "Setting Updated", f"{key} = {val}")
    await state.clear()
    await message.answer(f"✅ Setting <code>{key}</code> updated successfully.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Settings Menu", callback_data="admin_settings")]
    ]), parse_mode="HTML")

@dp.callback_query(F.data == "admin_logs")
async def cb_admin_logs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 15")
            logs = await cur.fetchall()

    if not logs:
        text = "📝 No admin logs recorded."
    else:
        text = "📝 <b>AUDIT LOGS (Last 15):</b>\n\n"
        for l in logs:
            text += f"• <code>{l['created_at'].strftime('%m-%d %H:%M')}</code> | <b>{html.escape(l['action'])}</b>: {html.escape(l['details'] or '')}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_logs"),
         InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id, username, first_name, created_at FROM users ORDER BY created_at DESC LIMIT 15")
            users = await cur.fetchall()

    text = "👥 <b>RECENT USERS (Last 15 Registered):</b>\n\n"
    for u in users:
        text += f"• <code>{u['id']}</code> - @{html.escape(u['username'] or 'N/A')} ({html.escape(u['first_name'] or '')})\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Admin Menu", callback_data="admin_dashboard")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "ignore")
async def cb_ignore(call: CallbackQuery):
    await call.answer()


# =============================================================================
# 19. STARTUP & SHUTDOWN HOOKS
# =============================================================================

async def main():
    logger.info("Starting ISell Store Bot initialization...")
    await init_db()

    # Register Bot Commands Menu for Telegram clients
    customer_commands = [
        BotCommand(command="start", description="Open main store"),
        BotCommand(command="store", description="Browse digital products"),
        BotCommand(command="orders", description="View your purchases"),
        BotCommand(command="profile", description="Your profile & stats"),
        BotCommand(command="help", description="Guide and support")
    ]
    await bot.set_my_commands(customer_commands)

    # Clean any dangling updates
    await bot.delete_webhook(drop_pending_updates=True)
    
    logger.info("Bot is polling for updates...")
    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            db_pool.close()
            await db_pool.wait_closed()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("ISell Store stopped.")