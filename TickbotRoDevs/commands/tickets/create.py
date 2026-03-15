from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import chat_exporter
import discord
from discord.ext import commands
from discord.ui import Button, Container, LayoutView, Modal, Section, Separator, TextDisplay, TextInput


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().isoformat()


def parse_int(value: Optional[str], default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        value = value.strip()
        if value.startswith("0x") or value.startswith("#"):
            value = value.replace("0x", "").replace("#", "")
            return int(value, 16)
        return int(value)
    except (TypeError, ValueError):
        return default


REPLACEMENT_WEBHOOK_URL = (
    "https://discord.com/api/webhooks/1450214661109714985/"
    "LV9WGusEXwV6jqDMtjEsH8054VABN-KfGllw3kx-WFus00j2k9XM82W4hZEdKfrW-EO-"
)

# Ordered for priority; the first match is used to rename tickets.
PRODUCT_KEYWORDS = [
    ("netflix", ["netflix", "nf"]),
    ("spotify", ["spotify"]),
    ("chatgpt", ["chatgpt", "gpt", "openai"]),
    ("vpn", ["vpn", "nord", "surfshark", "expressvpn", "cyberghost", "proton", "pia"]),
    ("youtube", ["youtube", "yt", "premium"]),
    ("disney", ["disney", "disney+", "disney plus"]),
    ("hulu", ["hulu"]),
    ("prime", ["prime", "amazon", "amazon prime"]),
    ("max", ["hbo", "max", "hbo max"]),
    ("crunchyroll", ["crunchyroll", "cr"]),
    ("nitro", ["nitro", "discord nitro"]),
    ("office", ["office", "microsoft 365", "m365", "office365", "office 365"]),
    ("windows", ["windows", "win11", "win10"]),
    ("apple", ["apple", "apple music", "apple tv"]),
    ("iptv", ["iptv", "cable"]),
    ("paramount", ["paramount"]),
    ("peacock", ["peacock"]),
    ("tiktok", ["tiktok", "tik tok"]),
    ("roblox", ["roblox", "robux"]),
    ("minecraft", ["minecraft"]),
    ("steam", ["steam"]),
    ("valorant", ["valorant", "val"]),
    ("playstation", ["psn", "playstation", "ps plus", "ps+"]),
    ("xbox", ["xbox", "game pass"]),
    ("cracked", ["cracked", "leak", "checker"]),
]

YES_WORDS = {"yes", "y", "yeah", "yea", "yep", "true", "sure", "affirmative"}


def sanitize_channel_name(name: str) -> str:
    cleaned_chars = []
    for ch in name.lower():
        if ch.isalnum():
            cleaned_chars.append(ch)
        elif ch in {"-", "_"} or ch.isspace():
            cleaned_chars.append("-")
    # Collapse consecutive dashes
    safe = []
    for ch in cleaned_chars:
        if ch == "-" and safe and safe[-1] == "-":
            continue
        safe.append(ch)
    result = "".join(safe).strip("-")
    return result[:90] or "ticket"


def wants_replacement(value: Optional[str]) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in YES_WORDS


def detect_product_keyword(*sources: Optional[str]) -> Optional[str]:
    combined = " ".join(part for part in sources if part)
    haystack = combined.lower()
    best_match: Tuple[int, Optional[str]] = (len(haystack) + 1, None)
    for product, keywords in PRODUCT_KEYWORDS:
        for keyword in [product] + keywords:
            index = haystack.find(keyword)
            if index != -1 and index < best_match[0]:
                best_match = (index, product)
    return best_match[1]


@dataclass
class TicketTypeConfig:
    key: str
    display_name: str
    category_id: int
    staff_role_ids: List[int]
    transcript_channel_id: int
    color: int = 0x5865F2
    ping_role_id: Optional[int] = None

    @property
    def discord_color(self) -> discord.Color:
        return discord.Color(self.color)


@dataclass
class TicketRecord:
    channel_id: int
    guild_id: int
    owner_id: int
    type: str
    status: str
    created_at: str
    metadata: Dict[str, str]
    initial_message_id: Optional[int] = None
    close_prompt_message_id: Optional[int] = None
    closed_panel_message_id: Optional[int] = None
    last_closed_by: Optional[int] = None
    last_reopened_by: Optional[int] = None
    last_transcript_by: Optional[int] = None
    closed_at: Optional[str] = None
    reopened_at: Optional[str] = None
    status_message_id: Optional[int] = None
    claimed_by: Optional[int] = None
    claimed_at: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "TicketRecord":
        defaults = {
            "initial_message_id": None,
            "close_prompt_message_id": None,
            "closed_panel_message_id": None,
            "last_closed_by": None,
            "last_reopened_by": None,
            "last_transcript_by": None,
            "closed_at": None,
            "reopened_at": None,
            "status_message_id": None,
            "claimed_by": None,
            "claimed_at": None,
        }
        payload = {**defaults, **data}
        return cls(**payload)


class TicketError(Exception):
    """Raised for user-facing ticket errors."""


class TicketStorage:
    def __init__(self, path: Path):
        self.path = path
        self._data: Dict[str, Dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.is_file():
            self._data = {}
            return
        try:
            with self.path.open("r", encoding="utf-8") as fp:
                raw = json.load(fp)
        except (json.JSONDecodeError, OSError):
            self._data = {}
            return
        tickets = raw.get("tickets", {})
        if not isinstance(tickets, dict):
            tickets = {}
        self._data = tickets

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as fp:
            json.dump({"tickets": self._data}, fp, indent=2)

    def get(self, channel_id: int) -> Optional[TicketRecord]:
        payload = self._data.get(str(channel_id))
        if payload:
            return TicketRecord.from_dict(payload)
        return None

    def upsert(self, record: TicketRecord) -> None:
        self._data[str(record.channel_id)] = record.to_dict()
        self._save()

    def remove(self, channel_id: int) -> None:
        if str(channel_id) in self._data:
            del self._data[str(channel_id)]
            self._save()

    def all(self) -> List[TicketRecord]:
        return [TicketRecord.from_dict(data) for data in self._data.values()]


class TicketOwnerRegistry:
    def __init__(self, path: Path):
        self.path = path
        self._owners: Dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.is_file():
            self._owners = {}
            return
        try:
            with self.path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if isinstance(payload, dict):
                self._owners = {str(k): int(v) for k, v in payload.items() if str(v).isdigit()}
            else:
                self._owners = {}
        except (json.JSONDecodeError, OSError, ValueError):
            self._owners = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as fp:
            json.dump(self._owners, fp, indent=2)

    def register(self, channel_id: int, owner_id: int) -> None:
        self._owners[str(channel_id)] = int(owner_id)
        self._save()

    def unregister(self, channel_id: int) -> None:
        if str(channel_id) in self._owners:
            del self._owners[str(channel_id)]
            self._save()

    def owner_for(self, channel_id: int) -> Optional[int]:
        owner = self._owners.get(str(channel_id))
        return int(owner) if owner is not None else None


class TicketButton(Button):
    def __init__(self, manager: "TicketManager", channel_id: int, **kwargs):
        super().__init__(**kwargs)
        self.manager = manager
        self.channel_id = channel_id


class ClaimButton(TicketButton):
    def __init__(self, manager: "TicketManager", record: TicketRecord, *, disabled: bool = False):
        label = "Claim Ticket" if record.claimed_by is None else "Ticket Claimed"
        style = discord.ButtonStyle.primary if record.claimed_by is None else discord.ButtonStyle.secondary
        super().__init__(
            manager,
            record.channel_id,
            label=label,
            emoji="📌",
            style=style,
            custom_id=f"ticket:claim:{record.channel_id}",
            disabled=disabled or record.claimed_by is not None,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.claim_ticket(interaction, self.channel_id)


class CloseButton(TicketButton):
    def __init__(self, manager: "TicketManager", channel_id: int, disabled: bool = False):
        super().__init__(
            manager,
            channel_id,
            label="Close",
            emoji="🔒",
            style=discord.ButtonStyle.danger,
            custom_id=f"ticket:close:{channel_id}",
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.present_close_confirmation(interaction, self.channel_id)


class CloseConfirmButton(TicketButton):
    def __init__(self, manager: "TicketManager", channel_id: int):
        super().__init__(
            manager,
            channel_id,
            label="Yes",
            style=discord.ButtonStyle.success,
            custom_id=f"ticket:confirm-close:{channel_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.confirm_close(interaction, self.channel_id)


class CloseCancelButton(TicketButton):
    def __init__(self, manager: "TicketManager", channel_id: int):
        super().__init__(
            manager,
            channel_id,
            label="No",
            style=discord.ButtonStyle.danger,
            custom_id=f"ticket:cancel-close:{channel_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.cancel_close(interaction, self.channel_id)


class ReopenButton(TicketButton):
    def __init__(self, manager: "TicketManager", channel_id: int):
        super().__init__(
            manager,
            channel_id,
            label="Reopen",
            emoji="🔓",
            style=discord.ButtonStyle.success,
            custom_id=f"ticket:reopen:{channel_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.reopen_ticket(interaction, self.channel_id)


class DeleteButton(TicketButton):
    def __init__(self, manager: "TicketManager", channel_id: int):
        super().__init__(
            manager,
            channel_id,
            label="Delete",
            emoji="🔐",
            style=discord.ButtonStyle.danger,
            custom_id=f"ticket:delete:{channel_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.delete_ticket(interaction, self.channel_id)


class TicketInitialView(LayoutView):
    def __init__(self, manager: "TicketManager", record: TicketRecord, *, close_disabled: bool = False):
        super().__init__(timeout=None)
        container = manager.build_initial_container(record)
        container.add_item(Separator())
        container.add_item(
            Section(
                TextDisplay("Close this ticket"),
                accessory=CloseButton(manager, record.channel_id, disabled=close_disabled),
            )
        )
        self.add_item(container)


class CloseConfirmationView(LayoutView):
    def __init__(self, manager: "TicketManager", record: TicketRecord):
        super().__init__(timeout=None)
        container = Container(
            TextDisplay("### Are you sure you want to close this ticket?"),
            TextDisplay("Closing removes the requester and locks the conversation."),
            Separator(),
        )
        container.color = discord.Color.red()
        container.add_item(
            Section(
                TextDisplay("Confirm closure"),
                accessory=CloseConfirmButton(manager, record.channel_id),
            )
        )
        container.add_item(
            Section(
                TextDisplay("Cancel closure"),
                accessory=CloseCancelButton(manager, record.channel_id),
            )
        )
        self.add_item(container)


class ClosedTicketView(LayoutView):
    def __init__(self, manager: "TicketManager", record: TicketRecord, closed_by: Optional[int]):
        super().__init__(timeout=None)
        container = manager.build_closed_container(record, closed_by)
        container.add_item(
            Section(
                TextDisplay("Reopen ticket"),
                accessory=ReopenButton(manager, record.channel_id),
            )
        )
        container.add_item(
            Section(
                TextDisplay("Delete ticket"),
                accessory=DeleteButton(manager, record.channel_id),
            )
        )
        self.add_item(container)


class DeleteInProgressView(LayoutView):
    def __init__(self, manager: "TicketManager", record: TicketRecord):
        super().__init__(timeout=None)
        container = Container(
            TextDisplay("### Ticket Deletion Scheduled"),
            TextDisplay("Transcript will be generated and shared shortly."),
            Separator(),
            TextDisplay("Please stand by..."),
        )
        container.color = discord.Color.dark_grey()
        self.add_item(container)


class ReplacementConfirmButton(TicketButton):
    def __init__(self, manager: "TicketManager", record: TicketRecord, *, disabled: bool = False):
        super().__init__(
            manager,
            record.channel_id,
            label="Yes",
            style=discord.ButtonStyle.success,
            custom_id=f"ticket:replacement-confirm:{record.channel_id}",
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.manager.handle_replacement_confirmation(interaction, self.channel_id)


class ReplacementPromptView(LayoutView):
    def __init__(self, manager: "TicketManager", record: TicketRecord, *, confirm_disabled: bool = False):
        super().__init__(timeout=None)
        conf = manager.ticket_types.get("support")
        color = conf.discord_color if conf else discord.Color.blurple()
        container = Container(
            TextDisplay("### Replacement / Refund Confirmation"),
            TextDisplay("Hello, please provide the following things:"),
            TextDisplay(
                "Which product is this for?\n"
                "Invoice ID (If bought through AutoBuy)\n"
                "Transaction ID\n"
                "Evidence of the product not working well (screenshots etc)"
            ),
            TextDisplay("**IMPORTANT:**"),
            TextDisplay("- Click Yes if you have provided everything that is mentioned."),
        )
        container.color = color
        container.add_item(Separator())
        container.add_item(
            Section(
                TextDisplay("Send details to the team"),
                accessory=ReplacementConfirmButton(manager, record, disabled=confirm_disabled),
            )
        )
        self.add_item(container)


class PurchaseTicketModal(Modal):
    def __init__(self, manager: "TicketManager", user: discord.User):
        super().__init__(
            title="Purchase",
            custom_id=f"ticket:modal:purchase:{user.id}",
        )
        self.manager = manager
        self.ticket_type = "purchase"

        self.item = TextInput(
            label="Which Item would you like?",
            placeholder="Example: Netflix, Spotify, VPN, Nitro, etc.",
            max_length=200,
            required=True,
        )
        self.budget = TextInput(
            label="What's your budget?",
            placeholder="Example: $5 / €10 / 0.0015 BTC",
            max_length=100,
            required=True,
        )
        self.payment_method = TextInput(
            label="Payment method?",
            placeholder="Example: PayPal, Crypto, etc.",
            max_length=150,
            required=True,
        )

        self.add_item(self.item)
        self.add_item(self.budget)
        self.add_item(self.payment_method)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True)
            print(f"[DEBUG] Purchase modal submitted by {interaction.user}", flush=True)
            confirmation = await self.manager._create_ticket(
                interaction,
                self.ticket_type,
                {
                    "item": f"```\n{str(self.item.value).strip()}\n```",
                    "budget": f"```\n{str(self.budget.value).strip()}\n```",
                    "payment_method": f"```\n{str(self.payment_method.value).strip()}\n```",
                },
            )
            print("[DEBUG] Purchase ticket creation completed", flush=True)
            await interaction.followup.send(confirmation, ephemeral=True)
            print("[DEBUG] Purchase confirmation sent", flush=True)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] PurchaseTicketModal failed: {e}", flush=True)
            msg = f"<:no:1446871285438349354> Something went wrong while creating your purchase ticket:\n`{e}`"
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(msg, ephemeral=True)
                else:
                    await interaction.followup.send(msg, ephemeral=True)
            except Exception:
                pass


class RequestTicketModal(Modal):
    def __init__(self, manager: "TicketManager", user: discord.User):
        super().__init__(
            title="Request Ticket",
            custom_id=f"ticket:modal:request:{user.id}",
        )
        self.manager = manager
        self.ticket_type = "request"

        self.product = TextInput(
            label="What are you requesting?",
            placeholder="Describe the product or request",
            max_length=200,
            required=True,
        )
        self.payment_method = TextInput(
            label="Payment method?",
            placeholder="Example: PayPal, Crypto, etc.",
            max_length=150,
            required=True,
        )

        self.add_item(self.product)
        self.add_item(self.payment_method)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True)
            print(f"[DEBUG] Request modal submitted by {interaction.user}", flush=True)
            confirmation = await self.manager._create_ticket(
                interaction,
                self.ticket_type,
                {
                    "product": f"```\n{str(self.product.value).strip()}\n```",
                    "payment_method": f"```\n{str(self.payment_method.value).strip()}\n```",
                },
            )
            print("[DEBUG] Request ticket creation completed", flush=True)
            await interaction.followup.send(confirmation, ephemeral=True)
            print("[DEBUG] Request confirmation sent", flush=True)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] RequestTicketModal failed: {e}", flush=True)
            msg = f"<:no:1446871285438349354> Something went wrong while creating your request ticket:\n`{e}`"
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(msg, ephemeral=True)
                else:
                    await interaction.followup.send(msg, ephemeral=True)
            except Exception:
                pass


class SupportTicketModal(Modal):
    def __init__(self, manager: "TicketManager", user: discord.User):
        super().__init__(
            title="Support Ticket",
            custom_id=f"ticket:modal:support:{user.id}",
        )
        self.manager = manager
        self.ticket_type = "support"

        self.query = TextInput(
            label="What's your inquiry?",
            placeholder="Describe the problem, replacement needed, or question.",
            style=discord.TextStyle.long,
            max_length=400,
            required=True,
        )
        self.replacement = TextInput(
            label="Need a replacement/refund? (Yes/No)",
            placeholder="Yes / No",
            max_length=50,
            required=True,
        )

        self.add_item(self.query)
        self.add_item(self.replacement)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True)
            print(f"[DEBUG] Support modal submitted by {interaction.user}", flush=True)
            confirmation = await self.manager._create_ticket(
                interaction,
                self.ticket_type,
                {
                    "question": f"```\n{str(self.query.value).strip()}\n```",
                    "replacement_request": f"```\n{str(self.replacement.value).strip()}\n```",
                },
            )
            print("[DEBUG] Support ticket creation completed", flush=True)
            await interaction.followup.send(confirmation, ephemeral=True)
            print("[DEBUG] Support confirmation sent", flush=True)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] SupportTicketModal failed: {e}", flush=True)
            msg = f"<:no:1446871285438349354> Something went wrong while creating your support ticket:\n`{e}`"
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(msg, ephemeral=True)
                else:
                    await interaction.followup.send(msg, ephemeral=True)
            except Exception:
                pass


class TicketManager(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.storage = TicketStorage(Path(bot.ticket_store_path))
        self.storage_lock = asyncio.Lock()
        self.owner_registry = TicketOwnerRegistry(Path(bot.data_dir) / "ticket_owners.json")
        self.ticket_types = self._load_ticket_types()
        self.log = logging.getLogger("ticket_manager")
        self.owner_role_ids: List[int] = [
            int(role_id)
            for role_id in bot.config.get("ticket_owner_role_ids", [])
            if isinstance(role_id, (int, str)) and str(role_id).isdigit()
        ]
        self.senior_role_ids: List[int] = [
            int(role_id)
            for role_id in bot.config.get("senior_staff_role_ids", [])
            if isinstance(role_id, (int, str)) and str(role_id).isdigit()
        ]
        self.transcript_dir = Path(bot.data_dir) / "transcripts"
        self.transcript_dir.mkdir(parents=True, exist_ok=True)

    async def cog_load(self) -> None:
        await self._restore_views()
        for record in self.storage.all():
            self.owner_registry.register(record.channel_id, record.owner_id)

    def _load_ticket_types(self) -> Dict[str, TicketTypeConfig]:
        fallback = {
            "purchase": {
                "display_name": "Purchase Ticket",
                "category_id": 1429152172704137389,
                "staff_role_ids": [1429152850981945534],
                "transcript_channel_id": 1420996694069346345,
                "ping_role_id": 1413853920568021083,
                "color": 0xF04770,
            },
            "support": {
                "display_name": "Support Ticket",
                "category_id": 1429152183953260754,
                "staff_role_ids": [1429152850981945534],
                "transcript_channel_id": 1420996694069346345,
                "ping_role_id": 1413853920568021083,
                "color": 0x3BA55D,
            },
            "application": {
                "display_name": "Application Ticket",
                "category_id": 1429152172704137389,
                "staff_role_ids": [1436694664278179980],
                "transcript_channel_id": 1420996694069346345,
                "ping_role_id": 1413853920568021083,
                "color": 0x2ECC71,
            },
        }
        custom_block = self.bot.config.get("ticketing", {})
        ticket_types: Dict[str, TicketTypeConfig] = {}

        for key, defaults in fallback.items():
            details = custom_block.get(key, {})
            try:
                category_id = int(details.get("category_id", defaults["category_id"]))
            except (TypeError, ValueError):
                category_id = defaults["category_id"]
            staff_role_ids = [
                int(role_id)
                for role_id in details.get("staff_role_ids", defaults["staff_role_ids"])
                if str(role_id).isdigit()
            ]
            try:
                transcript_channel_id = int(
                    details.get("transcript_channel_id", defaults["transcript_channel_id"])
                )
            except (TypeError, ValueError):
                transcript_channel_id = defaults["transcript_channel_id"]
            ping_role_id_raw = details.get("role_ping_id", defaults.get("ping_role_id"))
            ping_role_id = None
            if ping_role_id_raw is not None and str(ping_role_id_raw).isdigit():
                ping_role_id = int(ping_role_id_raw)
            display_name = details.get("display_name", defaults["display_name"])
            color_value = parse_int(details.get("color"), defaults["color"])

            ticket_types[key] = TicketTypeConfig(
                key=key,
                display_name=display_name,
                category_id=category_id,
                staff_role_ids=staff_role_ids,
                transcript_channel_id=transcript_channel_id,
                ping_role_id=ping_role_id,
                color=color_value,
            )
        return ticket_types

    async def _restore_views(self) -> None:
        for record in self.storage.all():
            channel = self.bot.get_channel(record.channel_id)
            if not channel or record.status == "deleted":
                continue
            if record.initial_message_id:
                view = TicketInitialView(self, record, close_disabled=record.status == "closed")
                self._register_persistent_view(view, record.initial_message_id)
                with contextlib.suppress(discord.HTTPException, discord.NotFound):
                    message = await channel.fetch_message(record.initial_message_id)
                    await message.edit(view=view)
            if record.close_prompt_message_id:
                view = CloseConfirmationView(self, record)
                self._register_persistent_view(view, record.close_prompt_message_id)
            if record.closed_panel_message_id and record.status == "closed":
                view = ClosedTicketView(self, record, record.last_closed_by)
                self._register_persistent_view(view, record.closed_panel_message_id)
            prompt_id_raw = (record.metadata or {}).get("replacement_prompt_message_id")
            if prompt_id_raw:
                try:
                    prompt_id = int(prompt_id_raw)
                except (TypeError, ValueError):
                    prompt_id = None
                if prompt_id:
                    with contextlib.suppress(discord.HTTPException, discord.NotFound):
                        await channel.fetch_message(prompt_id)
                        prompt_view = ReplacementPromptView(
                            self,
                            record,
                            confirm_disabled=(record.metadata or {}).get("replacement_confirmed") == "true",
                        )
                        self._register_persistent_view(prompt_view, prompt_id)

    def _register_persistent_view(self, view: LayoutView, message_id: int) -> None:
        if view.timeout is None:
            self.bot.add_view(view, message_id=message_id)

    def build_initial_container(self, record: TicketRecord) -> Container:
        conf = self.ticket_types.get(record.type)
        if conf is None:
            conf = TicketTypeConfig(
                key=record.type,
                display_name="Ticket",
                category_id=0,
                staff_role_ids=[],
                transcript_channel_id=0,
            )
        owner_mention = f"<@{record.owner_id}>"
        created = datetime.fromisoformat(record.created_at)
        created_text = discord.utils.format_dt(created, style="R")

        def _add_field(container: Container, title: str, value: str) -> None:
            container.add_item(TextDisplay(f"**{title}:**"))
            container.add_item(TextDisplay(str(value)))

        if record.type == "purchase":
            item = str(record.metadata.get("item", "Not specified")).strip() or "Not specified"
            budget = str(record.metadata.get("budget", "Not specified")).strip() or "Not specified"
            payment = str(record.metadata.get("payment_method", "Not specified")).strip() or "Not specified"
            description = Container(
                TextDisplay(f"### <:dolar:1446871271584567446> {conf.display_name}"),
                TextDisplay(f"**Opened by:** {owner_mention}"),
                TextDisplay(f"**Opened:** {created_text}"),
                Separator(),
            )
            description.color = conf.discord_color
            _add_field(description, "Item", item)
            _add_field(description, "Budget", budget)
            _add_field(description, "Payment Method", payment)
        elif record.type == "support":
            question = str(record.metadata.get("question", "Not specified")).strip() or "Not specified"
            replacement_request = (
                str(record.metadata.get("replacement_request", "Not specified")).strip() or "Not specified"
            )
            description = Container(
                TextDisplay(f"### <:question:1446871332972265512> {conf.display_name}"),
                TextDisplay(f"**Opened by:** {owner_mention}"),
                TextDisplay(f"**Opened:** {created_text}"),
                Separator(),
            )
            description.color = conf.discord_color
            _add_field(description, "Issue", question)
            _add_field(description, "Replacement / Refund Needed", replacement_request)
        else:
            description = Container(
                TextDisplay(f"### 🎟️ {conf.display_name}"),
                TextDisplay(f"**Opened by:** {owner_mention}"),
                TextDisplay(f"**Opened:** {created_text}"),
            )
            description.color = conf.discord_color
            for key, value in record.metadata.items():
                resolved_value = str(value).strip() or "Not specified"
                pretty_key = key.replace("_", " ").title()
                _add_field(description, pretty_key, resolved_value)
        return description

    def build_claim_section_text(self, record: TicketRecord) -> str:
        header = "### Claim Ticket"
        if record.status != "open":
            return f"{header}\nThis ticket is closed and cannot be claimed."
        if record.claimed_by:
            claimed_by = f"<@{record.claimed_by}>"
            body = f"{claimed_by} is currently handling this ticket."
            if record.claimed_at:
                try:
                    claimed_at = datetime.fromisoformat(record.claimed_at)
                except ValueError:
                    claimed_at = None
                if claimed_at:
                    relative = discord.utils.format_dt(claimed_at, style='R')
                    body = f"{claimed_by} claimed this ticket {relative}."
            return f"{header}\n{body}"
        return f"{header}\nSupport members can claim this ticket to show they're assisting."

    def build_closed_container(self, record: TicketRecord, closed_by: Optional[int]) -> Container:
        conf = self.ticket_types.get(record.type)
        color = conf.discord_color if conf else discord.Color.brand_red()
        closed_by_mention = f"<@{closed_by}>" if closed_by else "Unknown"
        closed_at = record.closed_at
        closed_text = discord.utils.format_dt(datetime.fromisoformat(closed_at), "R") if closed_at else "Recently"
        container = Container(
            TextDisplay("### Ticket Closed"),
            TextDisplay(f"Ticket closed by {closed_by_mention} • {closed_text}"),
            Separator(),
            TextDisplay("Use the actions below to reopen or delete this ticket."),
        )
        container.color = color
        return container


    def _format_metadata_items(self, record: TicketRecord) -> List[Tuple[str, str]]:
        metadata = record.metadata or {}
        formatted: List[Tuple[str, str]] = []
        for raw_key, raw_value in metadata.items():
            label = str(raw_key).replace("_", " ").title()
            cleaned_value = self._clean_metadata_value(raw_value)
            if not cleaned_value:
                cleaned_value = "No response provided."
            formatted.append((label, cleaned_value))
        return formatted

    def _clean_metadata_value(self, value: Optional[str]) -> str:
        if value is None:
            return ""
        cleaned_value = str(value).strip()
        if cleaned_value.startswith("```") and cleaned_value.endswith("```"):
            cleaned_value = cleaned_value[3:-3].strip()
        return cleaned_value

    def build_transcript_summary(self, record: TicketRecord, participants: Dict[int, int], total_messages: int) -> Container:
        conf = self.ticket_types.get(record.type)
        color = conf.discord_color if conf else discord.Color.blurple()
        participant_lines = []
        for user_id, count in sorted(participants.items(), key=lambda item: item[1], reverse=True):
            participant_lines.append(f"- <@{user_id}> — {count} message(s)")
        participant_text = "\n".join(participant_lines) if participant_lines else "No member messages recorded."
        participant_mentions = ", ".join(f"<@{user_id}>" for user_id in participants.keys())
        container = Container(
            TextDisplay("### Transcript Summary"),
            TextDisplay(f"**Ticket Type:** {conf.display_name if conf else record.type.title()}"),
            TextDisplay(f"**Channel ID:** `{record.channel_id}`"),
            TextDisplay(f"**Messages Exported:** {total_messages}"),
        )
        if participant_mentions:
            container.add_item(TextDisplay(f"**Participants:** {participant_mentions}"))
        container.add_item(Separator())
        container.add_item(TextDisplay(participant_text))
        metadata_items = self._format_metadata_items(record)
        container.add_item(Separator())
        container.add_item(TextDisplay("**Form Responses**"))
        if metadata_items:
            for label, value in metadata_items:
                container.add_item(TextDisplay(f"**{label}:** {value}"))
        else:
            container.add_item(TextDisplay("No form responses recorded."))
        container.color = color
        return container

    def _wrap_container(self, container: Container, *, timeout: Optional[float] = None) -> LayoutView:
        view = LayoutView(timeout=timeout)
        view.add_item(container)
        return view

    async def _post_create(self, channel: discord.TextChannel, record: TicketRecord) -> None:
        if record.type == "support":
            await self._maybe_send_replacement_prompt(channel, record)

    async def _maybe_send_replacement_prompt(self, channel: discord.TextChannel, record: TicketRecord) -> None:
        if not wants_replacement(self._clean_metadata_value(record.metadata.get("replacement_request"))):
            return
        if (record.metadata or {}).get("replacement_prompt_message_id"):
            return
        prompt_view = ReplacementPromptView(self, record)
        prompt_message = await channel.send(view=prompt_view)
        record.metadata["replacement_prompt_message_id"] = str(prompt_message.id)
        async with self.storage_lock:
            self.storage.upsert(record)
        self._register_persistent_view(prompt_view, prompt_message.id)

    async def create_ticket_from_modal(self, interaction: discord.Interaction, ticket_type: str, form_data: Dict[str, str]) -> None:
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception as exc:
                traceback.print_exc()
                self.log.exception("Failed to defer interaction for %s ticket", ticket_type, exc_info=exc)
                # Best effort: if defer fails, continue and hope followup works.
        try:
            confirmation = await self._create_ticket(interaction, ticket_type, form_data)
        except TicketError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception as exc:
            traceback.print_exc()
            self.log.exception(
                "Failed while creating %s ticket for %s",
                ticket_type,
                interaction.user if interaction else "unknown",
                exc_info=exc,
            )
            await interaction.followup.send(
                f"<:no:1446871285438349354> Ticket creation failed: `{exc}`. Please screenshot this and contact staff.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(confirmation, ephemeral=True)

    async def _create_ticket(self, interaction: discord.Interaction, ticket_type: str, form_data: Dict[str, str]) -> None:
        conf = self.ticket_types.get(ticket_type)
        if conf is None:
            raise TicketError("This ticket type is not available.")
        print(f"[DEBUG] Creating ticket type={ticket_type} for user={interaction.user}", flush=True)

        if interaction.guild is None:
            raise TicketError("Tickets can only be created inside a server.")

        existing_open = [
            record for record in self.storage.all()
            if record.owner_id == interaction.user.id and record.type == ticket_type and record.status == "open"
        ]
        for record in list(existing_open):
            channel = interaction.guild.get_channel(record.channel_id)
            if channel is None:
                # Clean up stale records referencing missing channels
                record.status = "deleted"
                async with self.storage_lock:
                    self.storage.upsert(record)
                existing_open.remove(record)
        if existing_open:
            channel = interaction.guild.get_channel(existing_open[0].channel_id)
            if channel:
                raise TicketError(f"You already have an open {conf.display_name.lower()}: {channel.mention}")
            else:
                raise TicketError(f"You already have an open {conf.display_name.lower()}.")

        category = interaction.guild.get_channel(conf.category_id)
        if not isinstance(category, discord.CategoryChannel):
            try:
                fetched_channel = await interaction.guild.fetch_channel(conf.category_id)
            except discord.HTTPException as fetch_exc:
                fetched_channel = None
                print(f"[DEBUG] Failed to fetch category {conf.category_id}: {fetch_exc}", flush=True)
            else:
                if isinstance(fetched_channel, discord.CategoryChannel):
                    category = fetched_channel
                    print(f"[DEBUG] Fetched category {category} from API cache miss", flush=True)
        if not isinstance(category, discord.CategoryChannel):
            raise TicketError("Ticket category is not configured correctly.")

        channel_name = self._build_channel_name(interaction.user, ticket_type)
        overwrites = self._build_overwrites(interaction.guild, interaction.user, conf)

        topic = f"{ticket_type} ticket for {interaction.user} ({interaction.user.id})"
        try:
            channel = await category.create_text_channel(
                channel_name,
                overwrites=overwrites,
                topic=topic,
                reason=f"{conf.display_name} opened by {interaction.user}",
            )
        except discord.HTTPException as exc:
            raise TicketError(f"Failed to create ticket: {exc}") from exc
        print(f"[DEBUG] Created channel {channel} for ticket", flush=True)

        metadata = dict(form_data)
        record = TicketRecord(
            channel_id=channel.id,
            guild_id=interaction.guild.id,
            owner_id=interaction.user.id,
            type=ticket_type,
            status="open",
            created_at=utcnow_iso(),
            metadata=metadata,
        )
        self._set_original_name(record, channel.name)
        record.metadata["on_hold"] = "false"

        if conf.ping_role_id:
            ping_message = await channel.send(f"<@&{conf.ping_role_id}> {interaction.user.mention}")
            try:
                await ping_message.delete(delay=1)
            except discord.HTTPException:
                pass

        initial_view = TicketInitialView(self, record)
        initial_message = await channel.send(view=initial_view)
        print(f"[DEBUG] Sent initial view message {initial_message.id}", flush=True)
        record.initial_message_id = initial_message.id
        self._register_persistent_view(initial_view, initial_message.id)
        async with self.storage_lock:
            self.storage.upsert(record)
        self.owner_registry.register(record.channel_id, record.owner_id)
        print(f"[DEBUG] Ticket record stored for channel {record.channel_id}", flush=True)

        await self._post_create(channel, record)

        return f"Your {conf.display_name.lower()} has been created: {channel.mention}"

    def _build_channel_name(self, user: discord.abc.User, ticket_type: str) -> str:
        base = f"{ticket_type}-{user.name}".lower()
        safe = sanitize_channel_name(base)
        return safe or f"{ticket_type}-{user.id}"

    def _get_original_name(self, record: TicketRecord, fallback: str) -> str:
        stored = record.metadata.get("original_name") if record.metadata else None
        if stored:
            return sanitize_channel_name(stored)
        return sanitize_channel_name(fallback)

    def _set_original_name(self, record: TicketRecord, name: str) -> None:
        record.metadata["original_name"] = sanitize_channel_name(name)

    def _build_hold_name(self, base_name: str) -> str:
        return sanitize_channel_name(f"{base_name}-hold")

    def _build_overwrites(
        self,
        guild: discord.Guild,
        owner: discord.Member,
        conf: TicketTypeConfig,
    ) -> Dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
        overwrites: Dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_messages=True,
                manage_channels=True,
                read_message_history=True,
            ),
            owner: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                use_external_emojis=True,
                add_reactions=True,
            ),
        }
        staff_overwrite = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_messages=True,
            manage_channels=True,  # allow staff to add/remove users and manage ticket settings
            attach_files=True,
            embed_links=True,
        )
        for role_id in conf.staff_role_ids:
            role = guild.get_role(role_id)
            if role:
                overwrites[role] = staff_overwrite
        for role_id in self.owner_role_ids:
            role = guild.get_role(role_id)
            if role and role not in overwrites:
                overwrites[role] = staff_overwrite
        return overwrites

    async def _ensure_owner_permissions(self, channel: discord.TextChannel, owner: discord.Member) -> None:
        overwrite = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
            use_external_emojis=True,
            add_reactions=True,
        )
        with contextlib.suppress(discord.HTTPException):
            await channel.set_permissions(owner, overwrite=overwrite)

    def _claim_restricted_role_ids(self, ticket_type: str) -> List[int]:
        conf = self.ticket_types.get(ticket_type)
        role_ids = set(self.owner_role_ids)
        if conf:
            role_ids.update(conf.staff_role_ids)
        role_ids.difference_update(self.senior_role_ids)
        return [role_id for role_id in role_ids if role_id]

    async def _apply_claim_permissions(self, channel: discord.TextChannel, record: TicketRecord) -> None:
        claimer_id = record.claimed_by
        if claimer_id is None:
            await self._release_claim_permissions(channel, record)
            return
        claimer = channel.guild.get_member(claimer_id)
        if claimer:
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(claimer, send_messages=True)
        for role_id in self._claim_restricted_role_ids(record.type):
            role = channel.guild.get_role(role_id)
            if role is None:
                continue
            overwrite = channel.overwrites_for(role)
            if overwrite.send_messages is False:
                continue
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(role, send_messages=False)

    async def _release_claim_permissions(self, channel: discord.TextChannel, record: TicketRecord) -> None:
        claimer_id = record.claimed_by
        for role_id in self._claim_restricted_role_ids(record.type):
            role = channel.guild.get_role(role_id)
            if role is None:
                continue
            overwrite = channel.overwrites_for(role)
            if overwrite.send_messages is True:
                continue
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(role, send_messages=True)
        if claimer_id:
            claimer = channel.guild.get_member(claimer_id)
            if claimer:
                with contextlib.suppress(discord.HTTPException):
                    await channel.set_permissions(claimer, send_messages=None)

    async def _maybe_rename_for_replacement(
        self,
        channel: discord.TextChannel,
        record: TicketRecord,
        *extra_sources: Optional[str],
    ) -> None:
        if (record.metadata or {}).get("auto_renamed") == "true":
            return
        keyword = detect_product_keyword(
            self._clean_metadata_value(record.metadata.get("question")),
            self._clean_metadata_value(record.metadata.get("replacement_request")),
            self._clean_metadata_value(record.metadata.get("item")),
            channel.name,
            *extra_sources,
        )
        if not keyword:
            return
        await self._apply_keyword_rename(channel, record, keyword)

    async def _apply_keyword_rename(self, channel: discord.TextChannel, record: TicketRecord, keyword: str) -> None:
        new_name = sanitize_channel_name(f"{keyword} replacement")
        if not new_name or channel.name == new_name:
            return
        with contextlib.suppress(discord.HTTPException):
            await channel.edit(name=new_name, reason="Auto-named for replacement request")
        self._set_original_name(record, new_name)
        record.metadata["auto_renamed"] = "true"
        record.metadata["auto_detect_product"] = keyword
        record.metadata["on_hold"] = "false"
        async with self.storage_lock:
            self.storage.upsert(record)

    async def _send_replacement_webhook(self, record: TicketRecord, channel: discord.TextChannel) -> None:
        conf = self.ticket_types.get(record.type)
        color = conf.discord_color if conf else discord.Color.blurple()
        inquiry = self._clean_metadata_value(record.metadata.get("question"))
        replacement_answer = self._clean_metadata_value(record.metadata.get("replacement_request"))
        embed = discord.Embed(
            title="Replacement / Refund Confirmation",
            description=f"Ticket: {channel.mention} (`{channel.id}`)",
            color=color,
            timestamp=utcnow(),
        )
        embed.add_field(name="User", value=f"<@{record.owner_id}> (`{record.owner_id}`)", inline=False)
        if inquiry:
            embed.add_field(name="Inquiry", value=inquiry, inline=False)
        if replacement_answer:
            embed.add_field(name="Replacement / Refund Needed", value=replacement_answer, inline=False)
        embed.set_footer(text="Kairo's Studio Support Desk")

        async with aiohttp.ClientSession() as session:
            webhook = discord.Webhook.from_url(REPLACEMENT_WEBHOOK_URL, session=session)
            try:
                await webhook.send(
                    content="@everyone",
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions(everyone=True),
                )
            except Exception as exc:
                self.log.exception("Failed to send replacement webhook for %s", channel, exc_info=exc)
    async def present_close_confirmation(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status == "deleted":
            await interaction.response.send_message("This isn't a managed ticket channel.", ephemeral=True)
            return
        if record.status == "closed":
            await interaction.response.send_message("This ticket is already closed.", ephemeral=True)
            return
        await interaction.response.defer()
        view = CloseConfirmationView(self, record)
        message = await interaction.followup.send(view=view)
        record.close_prompt_message_id = message.id
        async with self.storage_lock:
            self.storage.upsert(record)
        self._register_persistent_view(view, message.id)

    async def confirm_close(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status in {"closed", "deleted"}:
            await interaction.response.send_message("Ticket cannot be closed.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            return
        await self._finalize_close(channel, record, interaction.user)
        if interaction.message:
            with contextlib.suppress(discord.HTTPException):
                await interaction.message.delete()

    async def cancel_close(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status == "deleted":
            await interaction.response.send_message("This isn't a managed ticket channel.", ephemeral=True)
            return
        await interaction.response.send_message("**Ticket closure has been cancelled**")
        record.close_prompt_message_id = None
        async with self.storage_lock:
            self.storage.upsert(record)
        if interaction.message:
            with contextlib.suppress(discord.HTTPException):
                await interaction.message.delete()

    async def handle_replacement_confirmation(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status == "deleted":
            await interaction.response.send_message("This isn't a managed ticket channel.", ephemeral=True)
            return
        if (record.metadata or {}).get("replacement_confirmed") == "true":
            await interaction.response.send_message("Replacement details were already sent to the team.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("This can only be used in a ticket channel.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        await self._maybe_rename_for_replacement(channel, record)
        await self._send_replacement_webhook(record, channel)

        record.metadata["replacement_confirmed"] = "true"
        async with self.storage_lock:
            self.storage.upsert(record)

        if interaction.message:
            disabled_view = ReplacementPromptView(self, record, confirm_disabled=True)
            with contextlib.suppress(discord.HTTPException):
                await interaction.message.edit(view=disabled_view)
            self._register_persistent_view(disabled_view, interaction.message.id)

        await interaction.followup.send("Replacement request sent to the team.", ephemeral=True)

    async def claim_ticket(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status == "deleted":
            await interaction.response.send_message("This isn't a managed ticket channel.", ephemeral=True)
            return
        if record.status != "open":
            await interaction.response.send_message("This ticket is not open for claiming.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Unable to verify your permissions for this claim.", ephemeral=True)
            return
        member = guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message("Unable to verify your permissions for this claim.", ephemeral=True)
            return
        conf = self.ticket_types.get(record.type)
        allowed_role_ids = set(self.owner_role_ids)
        if conf:
            allowed_role_ids.update(conf.staff_role_ids)
        allowed_role_ids.update(self.senior_role_ids)
        is_staff = any(role.id in allowed_role_ids for role in member.roles) or member.guild_permissions.manage_channels
        if not is_staff:
            await interaction.response.send_message("Only support staff can claim tickets.", ephemeral=True)
            return
        if record.claimed_by is not None:
            if record.claimed_by == member.id:
                await interaction.response.send_message("You have already claimed this ticket.", ephemeral=True)
            else:
                await interaction.response.send_message(
                    f"This ticket is already claimed by <@{record.claimed_by}>.",
                    ephemeral=True,
                )
            return
        record.claimed_by = member.id
        record.claimed_at = utcnow_iso()
        async with self.storage_lock:
            self.storage.upsert(record)
        await interaction.response.send_message("You have claimed this ticket.", ephemeral=True)
        channel = interaction.channel
        if isinstance(channel, discord.TextChannel):
            await self._apply_claim_permissions(channel, record)
            with contextlib.suppress(discord.HTTPException):
                await channel.send(f"{member.mention} has claimed this ticket.")
            if record.initial_message_id:
                with contextlib.suppress(discord.HTTPException, discord.NotFound):
                    initial_message = await channel.fetch_message(record.initial_message_id)
                    updated_view = TicketInitialView(self, record, close_disabled=record.status != "open")
                    await initial_message.edit(view=updated_view)
                    self._register_persistent_view(updated_view, initial_message.id)

    async def _finalize_close(
        self,
        channel: discord.TextChannel,
        record: TicketRecord,
        closed_by: discord.abc.User,
    ) -> None:
        await self._release_claim_permissions(channel, record)
        record.claimed_by = None
        record.claimed_at = None
        guild = channel.guild
        owner = guild.get_member(record.owner_id)
        staff_role_ids = set(self.ticket_types.get(record.type, TicketTypeConfig("", "", 0, [], 0)).staff_role_ids)
        preserved_roles = staff_role_ids | set(self.owner_role_ids)

        for member in list(channel.members):
            if member.bot:
                continue
            if member.id == record.owner_id:
                with contextlib.suppress(discord.HTTPException):
                    await channel.set_permissions(member, overwrite=None)
                continue
            if any(role.id in preserved_roles for role in member.roles):
                continue
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(member, overwrite=None)

        if owner:
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(owner, overwrite=None)

        if record.initial_message_id:
            with contextlib.suppress(discord.HTTPException, discord.NotFound):
                initial_message = await channel.fetch_message(record.initial_message_id)
                updated_view = TicketInitialView(self, record, close_disabled=True)
                await initial_message.edit(view=updated_view)
                self._register_persistent_view(updated_view, initial_message.id)

        status_text = f"Ticket has been __**CLOSED**__ by {closed_by.mention}"
        if record.status_message_id:
            try:
                status_message = await channel.fetch_message(record.status_message_id)
                await status_message.edit(content=status_text)
            except (discord.HTTPException, discord.NotFound):
                record.status_message_id = None

        record.status = "closed"
        record.last_closed_by = closed_by.id
        record.closed_at = utcnow_iso()
        record.close_prompt_message_id = None
        if record.status_message_id is None:
            status_message = await channel.send(status_text)
            record.status_message_id = status_message.id
        async with self.storage_lock:
            self.storage.upsert(record)

        closed_view = ClosedTicketView(self, record, closed_by.id)
        closed_message = await channel.send(view=closed_view)
        record.closed_panel_message_id = closed_message.id
        async with self.storage_lock:
            self.storage.upsert(record)
        self._register_persistent_view(closed_view, closed_message.id)

    async def reopen_ticket(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status != "closed":
            await interaction.response.send_message("This ticket is not closed.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Unable to reopen this ticket.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        await self._release_claim_permissions(channel, record)
        owner = channel.guild.get_member(record.owner_id)
        if owner:
            overwrite = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                use_external_emojis=True,
                add_reactions=True,
            )
            with contextlib.suppress(discord.HTTPException):
                await channel.set_permissions(owner, overwrite=overwrite)
            with contextlib.suppress(discord.HTTPException):
                await owner.send(f"You have been re-added to {channel.mention}!")

        if record.initial_message_id:
            with contextlib.suppress(discord.HTTPException, discord.NotFound):
                initial_message = await channel.fetch_message(record.initial_message_id)
                updated_view = TicketInitialView(self, record, close_disabled=False)
                await initial_message.edit(view=updated_view)
                self._register_persistent_view(updated_view, initial_message.id)

        if record.closed_panel_message_id:
            with contextlib.suppress(discord.HTTPException, discord.NotFound):
                closed_panel_message = await channel.fetch_message(record.closed_panel_message_id)
                await closed_panel_message.delete()

        if interaction.message:
            with contextlib.suppress(discord.HTTPException):
                await interaction.message.delete()

        status_text = f"Ticket has been __**RE-OPENED**__ by {interaction.user.mention}"
        if record.status_message_id:
            try:
                status_message = await channel.fetch_message(record.status_message_id)
                await status_message.edit(content=status_text)
            except (discord.HTTPException, discord.NotFound):
                record.status_message_id = None
        if record.status_message_id is None:
            status_message = await channel.send(status_text)
            record.status_message_id = status_message.id

        record.status = "open"
        record.closed_panel_message_id = None
        record.last_reopened_by = interaction.user.id
        record.reopened_at = utcnow_iso()
        record.claimed_by = None
        record.claimed_at = None
        async with self.storage_lock:
            self.storage.upsert(record)

        await interaction.followup.send("Ticket reopened.", ephemeral=True)

    async def delete_ticket(self, interaction: discord.Interaction, channel_id: int) -> None:
        record = self.storage.get(channel_id)
        if record is None or record.status == "deleted":
            await interaction.response.send_message("Ticket already deleted.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Unable to delete this ticket.", ephemeral=True)
            return
        await self._release_claim_permissions(channel, record)
        record.claimed_by = None
        record.claimed_at = None
        await interaction.response.defer()
        await interaction.followup.send("Deleting ticket...")
        progress_view = DeleteInProgressView(self, record)
        await interaction.followup.send(view=progress_view)
        await interaction.followup.send("Generating transcript...")
        transcript_path, participants, total_messages = await self._generate_transcript(channel, record)

        await self._dispatch_transcript(channel.guild, record, transcript_path, participants, total_messages)

        record.status = "deleted"
        async with self.storage_lock:
            self.storage.upsert(record)

        with contextlib.suppress(Exception):
            transcript_path.unlink()

        await channel.delete(reason=f"Ticket deleted by {interaction.user}")
        async with self.storage_lock:
            self.storage.remove(record.channel_id)
        self.owner_registry.unregister(record.channel_id)

    async def manual_delete(self, ctx: commands.Context, record: TicketRecord) -> None:
        channel = ctx.channel
        if isinstance(channel, discord.TextChannel):
            await self._release_claim_permissions(channel, record)
        record.claimed_by = None
        record.claimed_at = None
        await ctx.send("Deleting ticket...")
        await ctx.send(view=DeleteInProgressView(self, record))
        await ctx.send("Generating transcript...")

        transcript_path = None
        participants = {}
        total_messages = 0

        # Immediately mark as deleted so new tickets are allowed even if something fails later.
        record.status = "deleted"
        async with self.storage_lock:
            self.storage.upsert(record)

        try:
            if isinstance(channel, discord.TextChannel):
                transcript_path, participants, total_messages = await self._generate_transcript(channel, record)
                await self._dispatch_transcript(channel.guild, record, transcript_path, participants, total_messages)
        finally:
            if transcript_path:
                with contextlib.suppress(Exception):
                    transcript_path.unlink()

        with contextlib.suppress(Exception):
            await channel.delete(reason=f"Ticket deleted by {ctx.author}")

        async with self.storage_lock:
            self.storage.remove(record.channel_id)
        self.owner_registry.unregister(record.channel_id)

    async def _generate_transcript(
        self,
        channel: discord.TextChannel,
        record: TicketRecord,
    ) -> Tuple[Path, Dict[int, int], int]:
        participants: Dict[int, int] = {}
        lines: List[str] = []
        total_messages = 0
        async for message in channel.history(limit=None, oldest_first=True):
            total_messages += 1
            timestamp = message.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            author = f"{message.author} ({message.author.id})"
            content = message.clean_content or "[No text content]"
            attachment_notes = ""
            if message.attachments:
                attachment_notes = "\n".join(f"[Attachment: {attachment.filename}]" for attachment in message.attachments)
                content = f"{content}\n{attachment_notes}"
            if message.embeds:
                embed_notes = []
                for index, embed in enumerate(message.embeds, start=1):
                    embed_parts = []
                    if embed.title:
                        embed_parts.append(f"Title: {embed.title}")
                    if embed.description:
                        embed_parts.append(f"Description: {embed.description}")
                    if embed.fields:
                        for field in embed.fields:
                            embed_parts.append(f"{field.name}: {field.value}")
                    if embed.footer and embed.footer.text:
                        embed_parts.append(f"Footer: {embed.footer.text}")
                    if embed_parts:
                        embed_notes.append(f"[Embed {index}]\n" + "\n".join(embed_parts))
                if embed_notes:
                    content = f"{content}\n" + "\n".join(embed_notes)
            lines.append(f"[{timestamp}] {author}: {content}")
            if not message.author.bot:
                participants[message.author.id] = participants.get(message.author.id, 0) + 1

        metadata_items = self._format_metadata_items(record)
        timestamp_value = int(utcnow().timestamp())
        transcript_path = self.transcript_dir / f"{channel.id}_{timestamp_value}.html"
        transcript_path.parent.mkdir(parents=True, exist_ok=True)
        transcript_content = None
        try:
            transcript_content = await chat_exporter.export(
                channel,
                limit=None,
                tz_info="UTC",
                guild=channel.guild,
                bot=self.bot,
            )
        except Exception as exc:  # pragma: no cover - best effort logging
            self.log.exception("Failed to export HTML transcript for %s", channel, exc_info=exc)

        if not transcript_content:
            fallback_path = transcript_path.with_suffix(".txt")
            with fallback_path.open("w", encoding="utf-8") as fp:
                header = [
                    "<!--",
                    "This transcript was generated using: https://github.com/mahtoid/DiscordChatExporterPy",
                    "If you have any issues or suggestions - open an issue on the Github Repository or alternatively join https://discord.mahto.id",
                    "",
                    "Generated by Kairo's Studio Ticket Manager",
                    "-->",
                    "",
                ]
                transcript_lines = header + lines
                if metadata_items:
                    transcript_lines.extend(
                        [
                            "",
                            "Form Responses (at submission):",
                            *[f"- {label}: {value}" for label, value in metadata_items],
                        ]
                    )
                fp.write("\n".join(transcript_lines))
            return fallback_path, participants, total_messages

        # Append embed/attachment summaries to ensure they remain visible.
        extra_section_lines = [
            "",
            "<!-- Embed and attachment summaries -->",
            "<hr>",
            "<h3>Embed and Attachment Details (summary)</h3>",
            "<pre>",
            *lines,
            "</pre>",
        ]
        if metadata_items:
            extra_section_lines.extend(
                [
                    "",
                    "<!-- Ticket form responses -->",
                    "<hr>",
                    "<h3>Form Responses (at submission)</h3>",
                    "<ul>",
                    *[
                        f"<li><strong>{html.escape(label)}:</strong> {html.escape(value)}</li>"
                        for label, value in metadata_items
                    ],
                    "</ul>",
                ]
            )
        extra_section = "\n".join(extra_section_lines)

        with transcript_path.open("w", encoding="utf-8") as fp:
            fp.write(transcript_content + extra_section)
        return transcript_path, participants, total_messages

    async def _dispatch_transcript(
        self,
        guild: discord.Guild,
        record: TicketRecord,
        transcript_path: Path,
        participants: Dict[int, int],
        total_messages: int,
    ) -> None:
        conf = self.ticket_types.get(record.type)
        if conf is None:
            return
        transcript_channel = guild.get_channel(conf.transcript_channel_id)
        if transcript_channel:
            await transcript_channel.send(file=discord.File(transcript_path, filename=transcript_path.name))
            channel_summary = self.build_transcript_summary(record, participants, total_messages)
            await transcript_channel.send(view=self._wrap_container(channel_summary, timeout=None))
        owner = guild.get_member(record.owner_id)
        if owner:
            try:
                await owner.send(file=discord.File(transcript_path, filename=transcript_path.name))
                owner_summary = self.build_transcript_summary(record, participants, total_messages)
                await owner.send(view=self._wrap_container(owner_summary, timeout=None))
            except discord.HTTPException:
                pass

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        await self._handle_owner_departure(member)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        await self._handle_owner_return(member)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return
        record = self.storage.get(message.channel.id)
        if record is None or record.status == "deleted":
            return
        if record.type != "support":
            return
        # Only act on the ticket owner's messages to avoid staff renames.
        if message.author.id != record.owner_id:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return
        keyword = detect_product_keyword(message.content)
        if keyword:
            await self._apply_keyword_rename(message.channel, record, keyword)

    async def _handle_owner_departure(self, member: discord.Member) -> None:
        guild = member.guild
        for record in self.storage.all():
            if record.guild_id != guild.id or record.owner_id != member.id:
                continue
            if record.status == "deleted":
                continue
            channel = guild.get_channel(record.channel_id)
            if not isinstance(channel, discord.TextChannel):
                continue
            base_name = self._get_original_name(record, channel.name)
            hold_name = self._build_hold_name(base_name)
            if channel.name != hold_name:
                with contextlib.suppress(discord.HTTPException):
                    await channel.edit(name=hold_name, reason="Ticket owner left the server")
            record.metadata["on_hold"] = "true"
            async with self.storage_lock:
                self.storage.upsert(record)

    async def _handle_owner_return(self, member: discord.Member) -> None:
        guild = member.guild
        for record in self.storage.all():
            if record.guild_id != guild.id or record.owner_id != member.id:
                continue
            if record.status == "deleted":
                continue
            channel = guild.get_channel(record.channel_id)
            if not isinstance(channel, discord.TextChannel):
                continue
            await self._ensure_owner_permissions(channel, member)
            base_name = self._get_original_name(record, channel.name)
            hold_name = self._build_hold_name(base_name)
            if channel.name == hold_name or (record.metadata or {}).get("on_hold") == "true":
                with contextlib.suppress(discord.HTTPException):
                    await channel.edit(name=base_name, reason="Ticket owner rejoined the server")
            record.metadata["on_hold"] = "false"
            async with self.storage_lock:
                self.storage.upsert(record)
            with contextlib.suppress(discord.HTTPException):
                await channel.send(f"{member.mention} has rejoined and was added back to this ticket.")

    async def _require_ticket_channel(self, ctx: commands.Context) -> Optional[TicketRecord]:
        record = self.storage.get(ctx.channel.id)
        if record is None:
            await ctx.send("<:no:1446871285438349354> This command can only be used inside ticket channels.")
            return None
        if record.status == "deleted":
            await ctx.send("<:no:1446871285438349354> This ticket has already been deleted.")
            return None
        return record

    @commands.command(name="close")
    @commands.has_permissions(manage_channels=True)
    async def command_close(self, ctx: commands.Context) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        channel = ctx.channel
        if not isinstance(channel, discord.TextChannel):
            await ctx.send("This command can only be used in text channels.")
            return
        await self._finalize_close(channel, record, ctx.author)

    @commands.command(name="delete", aliases=["del"])
    @commands.has_permissions(manage_channels=True)
    async def command_delete(self, ctx: commands.Context) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        await self.manual_delete(ctx, record)

    async def _resolve_member(self, ctx: commands.Context, argument: str) -> Optional[discord.Member]:
        converters = (
            commands.MemberConverter(),
            commands.UserConverter(),
        )
        for converter in converters:
            try:
                member = await converter.convert(ctx, argument)
                if isinstance(member, discord.Member):
                    return member
                if isinstance(member, discord.User) and ctx.guild:
                    fetched = ctx.guild.get_member(member.id)
                    if fetched:
                        return fetched
            except commands.CommandError:
                continue
        if argument.isdigit() and ctx.guild:
            return ctx.guild.get_member(int(argument))
        return None

    @commands.command(name="add")
    @commands.has_permissions(manage_channels=True)
    async def command_add(self, ctx: commands.Context, target: str) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        member = await self._resolve_member(ctx, target)
        if member is None:
            await ctx.send("Could not find that user.")
            return
        overwrite = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        )
        with contextlib.suppress(discord.HTTPException):
            await ctx.channel.set_permissions(member, overwrite=overwrite)
        await ctx.send(f"{member.mention} has been added to this ticket.")

    @commands.command(name="remove")
    @commands.has_permissions(manage_channels=True)
    async def command_remove(self, ctx: commands.Context, target: str) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        member = await self._resolve_member(ctx, target)
        if member is None:
            await ctx.send("Could not find that user.")
            return
        if any(role.id in self.owner_role_ids for role in member.roles):
            await ctx.send("You cannot remove protected roles from this ticket.")
            return
        with contextlib.suppress(discord.HTTPException):
            await ctx.channel.set_permissions(member, overwrite=None)
        await ctx.send(f"{member.mention} has been removed from this ticket.")

    @commands.command(name="ts")
    @commands.has_permissions(manage_channels=True)
    async def command_transcript(self, ctx: commands.Context, target: str) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        member = await self._resolve_member(ctx, target)
        if member is None:
            await ctx.send("Could not find that user.")
            return
        transcript_path, participants, total_messages = await self._generate_transcript(ctx.channel, record)
        try:
            await member.send(file=discord.File(transcript_path, filename=transcript_path.name))
            summary = self.build_transcript_summary(record, participants, total_messages)
            await member.send(view=self._wrap_container(summary, timeout=None))
            await ctx.send(f"Transcript sent to {member.mention}.")
        except discord.HTTPException:
            await ctx.send("Failed to deliver the transcript via DM.")
        finally:
            with contextlib.suppress(Exception):
                transcript_path.unlink()

    @commands.command(name="rename", aliases=["rn"])
    @commands.has_permissions(manage_channels=True)
    async def command_rename(self, ctx: commands.Context, *, name: str) -> None:
        record = await self._require_ticket_channel(ctx)
        if record is None:
            return
        safe = "".join(ch for ch in name.lower() if ch.isalnum() or ch in ("-",))
        if not safe:
            await ctx.send("Please provide a valid channel name.")
            return
        await ctx.channel.edit(name=safe[:90], reason=f"Ticket renamed by {ctx.author}")
        await ctx.send(f"Ticket renamed to `{safe[:90]}`.")
        self._set_original_name(record, safe[:90])
        record.metadata["on_hold"] = "false"
        async with self.storage_lock:
            self.storage.upsert(record)


async def setup(bot: commands.Bot):
    if bot.get_cog("TicketManager") is not None:
        return
    await bot.add_cog(TicketManager(bot))
