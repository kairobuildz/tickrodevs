from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from discord.ui import Container, LayoutView, Section, Separator, TextDisplay, Thumbnail

_USERS      = "https://users.roblox.com/v1"
_GAMES      = "https://games.roblox.com/v1"
_BADGES     = "https://badges.roblox.com/v1"
_FRIENDS    = "https://friends.roblox.com/v1"
_THUMBNAILS = "https://thumbnails.roblox.com/v1"
_PRESENCE   = "https://presence.roblox.com/v1"
_GROUPS     = "https://groups.roblox.com/v1"

_TIMEOUT = aiohttp.ClientTimeout(total=8)
_DEFAULT_AVATAR = "https://tr.rbxcdn.com/53eb9b17fe1432a809c73a13889b5006/420/420/Image/Png"


async def _get(session: aiohttp.ClientSession, url: str, **params) -> Optional[Dict]:
    try:
        async with session.get(url, params=params or None, timeout=_TIMEOUT) as r:
            if r.status == 200:
                return await r.json()
    except Exception:
        pass
    return None


async def _post(session: aiohttp.ClientSession, url: str, payload: Dict) -> Optional[Dict]:
    try:
        async with session.post(url, json=payload, timeout=_TIMEOUT) as r:
            if r.status == 200:
                return await r.json()
    except Exception:
        pass
    return None


async def resolve_user(session: aiohttp.ClientSession, username: str) -> Optional[Dict]:
    data = await _post(session, f"{_USERS}/usernames/users",
                       {"usernames": [username], "excludeBannedUsers": False})
    if data and data.get("data"):
        return data["data"][0]
    return None


async def get_user(session: aiohttp.ClientSession, uid: int) -> Optional[Dict]:
    return await _get(session, f"{_USERS}/users/{uid}")


async def get_presence(session: aiohttp.ClientSession, uid: int) -> Optional[Dict]:
    data = await _post(session, f"{_PRESENCE}/presence/users", {"userIds": [uid]})
    if data and data.get("userPresences"):
        return data["userPresences"][0]
    return None


async def get_avatar_headshot(session: aiohttp.ClientSession, uid: int) -> Optional[str]:
    data = await _get(session, f"{_THUMBNAILS}/users/avatar-headshot",
                      userIds=uid, size="150x150", format="Png", isCircular="false")
    if data and data.get("data"):
        entry = data["data"][0]
        if entry.get("state") == "Completed":
            return entry.get("imageUrl")
    return None


async def get_friends(session: aiohttp.ClientSession, uid: int) -> Optional[Dict]:
    return await _get(session, f"{_FRIENDS}/users/{uid}/friends")


async def get_friend_count(session: aiohttp.ClientSession, uid: int) -> int:
    data = await _get(session, f"{_FRIENDS}/users/{uid}/friends/count")
    return (data or {}).get("count", 0)


async def get_follower_count(session: aiohttp.ClientSession, uid: int) -> int:
    data = await _get(session, f"{_FRIENDS}/users/{uid}/followers/count")
    return (data or {}).get("count", 0)


async def get_following_count(session: aiohttp.ClientSession, uid: int) -> int:
    data = await _get(session, f"{_FRIENDS}/users/{uid}/followings/count")
    return (data or {}).get("count", 0)


async def get_badges(session: aiohttp.ClientSession, uid: int, limit: int = 10) -> Optional[Dict]:
    return await _get(session, f"{_BADGES}/users/{uid}/badges", limit=limit, sortOrder="Desc")


async def get_groups(session: aiohttp.ClientSession, uid: int) -> Optional[Dict]:
    return await _get(session, f"{_GROUPS}/users/{uid}/groups/roles")


async def get_game(session: aiohttp.ClientSession, universe_id: int) -> Optional[Dict]:
    data = await _get(session, f"{_GAMES}/games", universeIds=universe_id)
    if data and data.get("data"):
        return data["data"][0]
    return None


async def get_game_icon(session: aiohttp.ClientSession, universe_id: int) -> Optional[str]:
    data = await _get(session, f"{_THUMBNAILS}/games/icons",
                      universeIds=universe_id, returnPolicy="PlaceHolder",
                      size="150x150", format="Png", isCircular="false")
    if data and data.get("data"):
        entry = data["data"][0]
        if entry.get("state") == "Completed":
            return entry.get("imageUrl")
    return None


async def get_game_votes(session: aiohttp.ClientSession, universe_id: int) -> Optional[Dict]:
    data = await _get(session, f"{_GAMES}/games/votes", universeIds=universe_id)
    if data and data.get("data"):
        return data["data"][0]
    return None


def _n(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _pct(up: int, down: int) -> str:
    total = up + down
    if total == 0:
        return "N/A"
    return f"{up / total * 100:.1f}%"


_PRESENCE_MAP = {0: "⚫ Offline", 1: "🟢 Online", 2: "🟣 In-Game", 3: "🔵 In Studio"}

def _presence(p_type: int) -> str:
    return _PRESENCE_MAP.get(p_type, "⚫ Offline")


def _make_layout(container: Container) -> LayoutView:
    view = LayoutView(timeout=None)
    view.add_item(container)
    return view


def build_user_view(
    user: Dict,
    presence: Optional[Dict],
    avatar_url: Optional[str],
    friends: int,
    followers: int,
    following: int,
    groups: Optional[List[Dict]],
    recent_badges: Optional[List[Dict]],
) -> LayoutView:
    uid          = user.get("id", "?")
    display_name = user.get("displayName") or user.get("name", "Unknown")
    username     = user.get("name", "?")
    bio          = (user.get("description") or "").strip()
    created_raw  = user.get("created", "")
    is_banned    = user.get("isBanned", False)
    has_premium  = user.get("hasVerifiedBadge", False)
    profile_url  = f"https://www.roblox.com/users/{uid}/profile"

    created_text = ""
    if created_raw:
        try:
            dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            created_text = discord.utils.format_dt(dt, "D")
        except Exception:
            created_text = created_raw[:10]

    status = _presence(presence.get("userPresenceType", 0)) if presence else "⚫ Offline"
    in_game = (presence or {}).get("lastLocation", "")

    badges_str = "✅" if has_premium else ""
    ban_str    = "🚫 " if is_banned else ""

    header_text = (
        f"### {ban_str}{display_name} {badges_str}\n"
        f"@{username} · `{uid}`\n"
        f"{status}{f' · {in_game}' if in_game and 'Website' not in in_game else ''}"
    )

    container = Container(
        Section(
            TextDisplay(header_text),
            accessory=Thumbnail(media=avatar_url or _DEFAULT_AVATAR),
        )
    )
    container.color = discord.Color(0xFF0000)

    if bio:
        container.add_item(Separator())
        container.add_item(TextDisplay(bio[:300] + ("…" if len(bio) > 300 else "")))

    container.add_item(Separator())
    container.add_item(TextDisplay(
        f"👥 **{_n(friends)}** friends · "
        f"👁️ **{_n(followers)}** followers · "
        f"➡️ **{_n(following)}** following"
        + (f"\n📅 Joined {created_text}" if created_text else "")
    ))

    if groups:
        container.add_item(Separator())
        lines = [f"**Groups ({len(groups)})**"]
        for g in groups[:5]:
            info = g.get("group", {})
            role = g.get("role", {})
            lines.append(f"· {info.get('name', '?')} — {role.get('name', '?')}")
        if len(groups) > 5:
            lines.append(f"· …and {len(groups) - 5} more")
        container.add_item(TextDisplay("\n".join(lines)))

    if recent_badges:
        container.add_item(Separator())
        lines = [f"**Recent Badges ({len(recent_badges)})**"]
        for b in recent_badges[:5]:
            lines.append(f"· {b.get('name', '?')}")
        container.add_item(TextDisplay("\n".join(lines)))

    container.add_item(Separator())
    container.add_item(TextDisplay(f"[**View Profile**]({profile_url})"))

    return _make_layout(container)


def build_game_view(
    game: Dict,
    icon_url: Optional[str],
    votes: Optional[Dict],
) -> LayoutView:
    universe_id  = game.get("id", "?")
    name         = game.get("name", "Unknown Game")
    description  = (game.get("description") or "").strip()
    playing      = game.get("playing", 0)
    visits       = game.get("visits", 0)
    max_players  = game.get("maxPlayers", 0)
    favs         = game.get("favoritedCount", 0)
    is_public    = game.get("isPublic", True)
    creator      = (game.get("creator") or {}).get("name", "Unknown")
    up           = (votes or {}).get("upVotes", 0)
    down         = (votes or {}).get("downVotes", 0)
    game_url     = f"https://www.roblox.com/games/{universe_id}"

    header_text = (
        f"### 🎮 {name}\n"
        f"by {creator} · {'🟢 Public' if is_public else '🔒 Private'}"
    )

    container = Container(
        Section(
            TextDisplay(header_text),
            accessory=Thumbnail(media=icon_url or _DEFAULT_AVATAR),
        )
    )
    container.color = discord.Color(0xFF0000)

    if description:
        container.add_item(Separator())
        container.add_item(TextDisplay(description[:300] + ("…" if len(description) > 300 else "")))

    container.add_item(Separator())
    container.add_item(TextDisplay(
        f"👥 **{_n(playing)}** playing · "
        f"🔢 **{_n(visits)}** visits · "
        f"⭐ **{_n(favs)}** favorites\n"
        f"👍 **{_n(up)}** · 👎 **{_n(down)}** · ✅ **{_pct(up, down)}** · 👤 max **{max_players}**"
    ))
    container.add_item(Separator())
    container.add_item(TextDisplay(f"[**View Game**]({game_url})"))

    return _make_layout(container)


def build_friends_view(user: Dict, friends_data: List[Dict], avatar_url: Optional[str]) -> LayoutView:
    uid         = user.get("id", "?")
    username    = user.get("name", "?")
    total       = len(friends_data)
    profile_url = f"https://www.roblox.com/users/{uid}/profile"

    container = Container(
        Section(
            TextDisplay(f"### 👥 {username}'s Friends\n`{uid}` · {_n(total)} friends"),
            accessory=Thumbnail(media=avatar_url or _DEFAULT_AVATAR),
        )
    )
    container.color = discord.Color(0xFF0000)
    container.add_item(Separator())

    if not friends_data:
        container.add_item(TextDisplay("No friends found or list is private."))
    else:
        lines = []
        for f in friends_data[:20]:
            fname = f.get("displayName") or f.get("name", "?")
            fuser = f.get("name", "?")
            fid   = f.get("id", "?")
            dot   = "🟢" if f.get("isOnline", False) else "⚫"
            lines.append(f"{dot} **{fname}** (@{fuser}) `{fid}`")
        container.add_item(TextDisplay("\n".join(lines)))
        if total > 20:
            container.add_item(TextDisplay(f"…and {total - 20} more"))

    container.add_item(Separator())
    container.add_item(TextDisplay(f"[**View Profile**]({profile_url})"))

    return _make_layout(container)


def build_badges_view(user: Dict, badges: List[Dict], avatar_url: Optional[str]) -> LayoutView:
    uid         = user.get("id", "?")
    uname       = user.get("name", "?")
    profile_url = f"https://www.roblox.com/users/{uid}/profile"

    container = Container(
        Section(
            TextDisplay(f"### 🏅 {uname}'s Badges\n`{uid}` · {len(badges)} shown"),
            accessory=Thumbnail(media=avatar_url or _DEFAULT_AVATAR),
        )
    )
    container.color = discord.Color(0xFF0000)
    container.add_item(Separator())

    if not badges:
        container.add_item(TextDisplay("No badges found."))
    else:
        lines = []
        for b in badges:
            bname   = b.get("name", "?")
            bdesc   = (b.get("description") or "").strip()
            awarded = b.get("statistics", {}).get("awardedCount", 0)
            line = f"**{bname}**"
            if bdesc:
                line += f" — {bdesc[:60]}{'…' if len(bdesc) > 60 else ''}"
            line += f"\n🏆 {_n(awarded)} players"
            lines.append(line)
        container.add_item(TextDisplay("\n\n".join(lines)))

    container.add_item(Separator())
    container.add_item(TextDisplay(f"[**View Profile**]({profile_url})"))

    return _make_layout(container)


def build_error_view(message: str) -> LayoutView:
    container = Container(TextDisplay(f"❌ {message}"))
    container.color = discord.Color.brand_red()
    return _make_layout(container)


class RobloxLookup(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    roblox = app_commands.Group(name="roblox", description="Look up anything on Roblox")

    @roblox.command(name="user", description="Look up a Roblox user profile")
    @app_commands.describe(username="Roblox username to search")
    async def roblox_user(self, interaction: discord.Interaction, username: str) -> None:
        await interaction.response.defer()
        async with aiohttp.ClientSession() as session:
            basic = await resolve_user(session, username)
            if not basic:
                await interaction.followup.send(view=build_error_view(f"No user found for **{username}**."), ephemeral=True)
                return
            uid = basic["id"]
            (user, presence, avatar_url, friends, followers, following, groups_raw, badges_raw) = await asyncio.gather(
                get_user(session, uid),
                get_presence(session, uid),
                get_avatar_headshot(session, uid),
                get_friend_count(session, uid),
                get_follower_count(session, uid),
                get_following_count(session, uid),
                get_groups(session, uid),
                get_badges(session, uid, limit=5),
            )
        await interaction.followup.send(view=build_user_view(
            user=user or basic,
            presence=presence,
            avatar_url=avatar_url,
            friends=friends,
            followers=followers,
            following=following,
            groups=(groups_raw or {}).get("data", []),
            recent_badges=(badges_raw or {}).get("data", []),
        ))

    @roblox.command(name="game", description="Look up a Roblox game by Universe ID")
    @app_commands.describe(universe_id="Universe ID of the game")
    async def roblox_game(self, interaction: discord.Interaction, universe_id: int) -> None:
        await interaction.response.defer()
        async with aiohttp.ClientSession() as session:
            game, icon_url, votes = await asyncio.gather(
                get_game(session, universe_id),
                get_game_icon(session, universe_id),
                get_game_votes(session, universe_id),
            )
        if not game:
            await interaction.followup.send(view=build_error_view(f"No game found for Universe ID **{universe_id}**."), ephemeral=True)
            return
        await interaction.followup.send(view=build_game_view(game=game, icon_url=icon_url, votes=votes))

    @roblox.command(name="friends", description="Show a Roblox user's friend list")
    @app_commands.describe(username="Roblox username to look up")
    async def roblox_friends(self, interaction: discord.Interaction, username: str) -> None:
        await interaction.response.defer()
        async with aiohttp.ClientSession() as session:
            basic = await resolve_user(session, username)
            if not basic:
                await interaction.followup.send(view=build_error_view(f"No user found for **{username}**."), ephemeral=True)
                return
            uid = basic["id"]
            user, friends_data, avatar_url = await asyncio.gather(
                get_user(session, uid),
                get_friends(session, uid),
                get_avatar_headshot(session, uid),
            )
        await interaction.followup.send(view=build_friends_view(
            user=user or basic,
            friends_data=(friends_data or {}).get("data", []),
            avatar_url=avatar_url,
        ))

    @roblox.command(name="badges", description="Show a Roblox user's badges")
    @app_commands.describe(username="Roblox username to look up")
    async def roblox_badges(self, interaction: discord.Interaction, username: str) -> None:
        await interaction.response.defer()
        async with aiohttp.ClientSession() as session:
            basic = await resolve_user(session, username)
            if not basic:
                await interaction.followup.send(view=build_error_view(f"No user found for **{username}**."), ephemeral=True)
                return
            uid = basic["id"]
            user, badges_raw, avatar_url = await asyncio.gather(
                get_user(session, uid),
                get_badges(session, uid, limit=20),
                get_avatar_headshot(session, uid),
            )
        await interaction.followup.send(view=build_badges_view(
            user=user or basic,
            badges=(badges_raw or {}).get("data", []),
            avatar_url=avatar_url,
        ))


async def setup(bot: commands.Bot) -> None:
    if bot.get_cog("RobloxLookup") is not None:
        return
    await bot.add_cog(RobloxLookup(bot))
    try:
        await bot.tree.sync()
    except Exception as exc:
        print(f"[WARN] Failed to sync slash commands: {exc}", flush=True)