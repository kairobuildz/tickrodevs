import importlib.util
import json
import os
import sys
from pathlib import Path
import functools
import logging

import discord
from discord.ext import commands

# --- DEBUG SETUP ---
print = functools.partial(print, flush=True)

logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)
# --- END DEBUG SETUP ---

# Load config
CONFIG_PATH = Path("config.json")
if not CONFIG_PATH.is_file():
    raise FileNotFoundError("config.json is missing!")

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    config = json.load(f)

TOKEN = config.get("token")
PREFIX = config.get("prefix")
WHITELISTED_ROLES = set(config.get("whitelisted_role_ids", []))
BRAND_NAME = config.get("brand_name", "Kairo's Studio")
ACTIVITY_TEXT = config.get("activity_text", f"{BRAND_NAME} Support")
DATA_DIRECTORY = Path(config.get("data_directory", "data"))

if not TOKEN:
    raise ValueError("Bot token is missing in config.json")
if not PREFIX:
    raise ValueError("Command prefix is missing in config.json")

DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)
bot.config = config
bot.brand_name = BRAND_NAME
bot.logs_channel_id = config.get("logs_channel_id")
bot.data_dir = DATA_DIRECTORY
bot.ticket_store_path = DATA_DIRECTORY / "tickets.json"
bot.whitelisted_roles = WHITELISTED_ROLES


def _debug_resource_status(label, identifier, resolved):
    if not identifier:
        print(f"[DEBUG] {label}: not configured")
        return
    if resolved is None:
        print(f"[DEBUG] {label} ({identifier}) -> MISSING")
    else:
        descriptor = getattr(resolved, "mention", None) or getattr(resolved, "name", None) or repr(resolved)
        print(f"[DEBUG] {label} ({identifier}) -> OK ({descriptor})")


def _log_guild_configuration(bot_instance, guild):
    print(f"[DEBUG] Validating configuration for guild {guild.name} ({guild.id})")
    manager = bot_instance.get_cog("TicketManager")

    if bot_instance.logs_channel_id:
        logs_channel = guild.get_channel(bot_instance.logs_channel_id)
        _debug_resource_status("Logs channel", bot_instance.logs_channel_id, logs_channel)

    for role_id in WHITELISTED_ROLES:
        role = guild.get_role(role_id)
        _debug_resource_status("Whitelisted role", role_id, role)

    if manager:
        for owner_role_id in getattr(manager, "owner_role_ids", []):
            role = guild.get_role(owner_role_id)
            _debug_resource_status("Ticket owner role", owner_role_id, role)

        for ticket_type, conf in manager.ticket_types.items():
            category = guild.get_channel(conf.category_id)
            category_resolved = category if isinstance(category, discord.CategoryChannel) else None
            _debug_resource_status(f"[{ticket_type}] Category", conf.category_id, category_resolved)

            transcript_channel = guild.get_channel(conf.transcript_channel_id)
            transcript_resolved = transcript_channel if isinstance(transcript_channel, discord.TextChannel) else None
            _debug_resource_status(f"[{ticket_type}] Transcript channel", conf.transcript_channel_id, transcript_resolved)

            for staff_role_id in conf.staff_role_ids:
                role = guild.get_role(staff_role_id)
                _debug_resource_status(f"[{ticket_type}] Staff role", staff_role_id, role)

            if conf.ping_role_id:
                ping_role = guild.get_role(conf.ping_role_id)
                _debug_resource_status(f"[{ticket_type}] Ping role", conf.ping_role_id, ping_role)


@bot.event
async def on_connect():
    print("🛰️ Connected to Discord Gateway...")


@bot.check
def check_whitelisted(ctx):
    if ctx.guild is None:
        return False
    if ctx.author.guild_permissions.administrator:
        return True
    author_roles = {role.id for role in ctx.author.roles}
    return bool(author_roles & WHITELISTED_ROLES)

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"🔒 Whitelisted role IDs: {WHITELISTED_ROLES}")
    print(f"📁 Data directory: {DATA_DIRECTORY.resolve()}")

    for guild in bot.guilds:
        _log_guild_configuration(bot, guild)
    
    # Set bot status
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=ACTIVITY_TEXT
        )
    )
    
    # Auto-load all cogs from commands/
    cog_count = 0
    commands_dirs = ["commands", "Commands"]  # Prefer lowercase, fallback to legacy
    
    # Find the correct commands directory
    commands_dir = None
    for dir_name in commands_dirs:
        if os.path.isdir(dir_name):
            commands_dir = dir_name
            break
    
    if commands_dir is None:
        print("<:no:1446871285438349354> Commands directory not found!")
        return
        
    package_root = commands_dir.replace(os.sep, ".")

    # Load commands from subdirectories
    for category in os.listdir(commands_dir):
        category_path = os.path.join(commands_dir, category)
        if not os.path.isdir(category_path):
            continue
        module_base = f"{package_root}.{category}"
        for filename in os.listdir(category_path):
            if not filename.endswith(".py") or filename.startswith("__"):
                continue
            module_name = f"{module_base}.{filename[:-3]}"
            spec = importlib.util.spec_from_file_location(
                module_name,
                os.path.join(category_path, filename)
            )
            if not spec or not spec.loader:
                continue
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            try:
                if hasattr(module, "setup"):
                    setup_result = module.setup(bot)
                    if setup_result is not None and hasattr(setup_result, "__await__"):
                        await setup_result
                    cog_count += 1
                    print(f"📦 Loaded cog: {module_name}")
            except Exception as e:
                print(f"<:no:1446871285438349354> Error loading {module_name}: {str(e)}")
    print(f"✨ Loaded {cog_count} command module(s).")
    print("Bot is ready!")

if __name__ == "__main__":
    print("🚀 Starting bot...")
    bot.run(TOKEN)
