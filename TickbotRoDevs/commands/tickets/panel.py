from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands
from discord.ui import Button, Container, LayoutView, Section, Select, Separator, TextDisplay, View

from .create import PurchaseTicketModal, SupportTicketModal, parse_int


def requires_whitelisted():
    async def predicate(ctx: commands.Context) -> bool:
        if ctx.guild is None:
            raise commands.CheckFailure("This command can only be used in a server.")
        if ctx.author.guild_permissions.administrator:
            return True
        allowed_roles = set(getattr(ctx.bot, "whitelisted_roles", set()))
        if not allowed_roles:
            raise commands.CheckFailure("Whitelisted roles are not configured.")
        author_role_ids = {role.id for role in getattr(ctx.author, "roles", [])}
        if author_role_ids & allowed_roles:
            return True
        raise commands.CheckFailure("You do not have permission to use this command.")
    return commands.check(predicate)


def resolve_emoji(value: Optional[str], fallback: str) -> str:
    if not value:
        return fallback
    if isinstance(value, (discord.Emoji, discord.PartialEmoji)):
        return value
    if isinstance(value, str):
        try:
            parsed = discord.PartialEmoji.from_str(value)
            if parsed.id is None and value.startswith(":") and value.endswith(":"):
                return fallback
            return parsed
        except (TypeError, ValueError):
            pass
        if value.startswith(":") and value.endswith(":"):
            return fallback
        return value
    return fallback


class TicketCategorySelect(Select):
    def __init__(self, manager, requester: discord.User):
        options = [
            discord.SelectOption(
                label="Purchase Desk",
                value="purchase",
                emoji=resolve_emoji("<:dolar:1446871271584567446>", "💵"),
                description="Quotes, payment methods, and order status updates.",
            ),
            discord.SelectOption(
                label="Support Desk",
                value="support",
                emoji=resolve_emoji("<:question:1446871332972265512>", "❓"),
                description="Troubleshooting, replacements, and account guidance.",
            ),
        ]
        super().__init__(
            placeholder="Choose a ticket route...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="ticket:panel:select",
        )
        self.manager = manager
        self.requester = requester

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Use your own selector to open a ticket.", ephemeral=True)
            return
        if self.manager is None:
            await interaction.response.send_message("Ticket system is not ready yet.", ephemeral=True)
            return
        choice = self.values[0]
        if choice == "purchase":
            await interaction.response.send_modal(PurchaseTicketModal(self.manager, interaction.user))
        elif choice == "support":
            await interaction.response.send_modal(SupportTicketModal(self.manager, interaction.user))


class TicketSelectView(View):
    def __init__(self, manager, requester: discord.User):
        super().__init__(timeout=120)
        self.add_item(TicketCategorySelect(manager, requester))


class MakeTicketButton(Button):
    def __init__(self):
        super().__init__(
            label="Open a Ticket",
            emoji=resolve_emoji("<:boost:1446871330111885446>", "🎫"),
            style=discord.ButtonStyle.primary,
            custom_id="ticket:panel:open-dropdown",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        manager = interaction.client.get_cog("TicketManager")
        if manager is None:
            await interaction.response.send_message("Ticket system is not ready yet.", ephemeral=True)
            return
        selector_view = TicketSelectView(manager, interaction.user)
        await interaction.response.send_message(
            "Select the ticket type that matches your request:",
            view=selector_view,
            ephemeral=True,
        )


def _build_panel_container(bot: commands.Bot) -> Container:
    brand_name  = getattr(bot, "brand_name", "Kairo's Studio")
    panel_color = discord.Color(parse_int(bot.config.get("ticket_panel_color"), 0x5865F2))
    description = bot.config.get(
        "ticket_panel_description",
        "Choose the ticket type that best matches your request.",
    )

    container = Container(
        TextDisplay(f"### <:speaker:1446871287954804840> {brand_name}"),
        TextDisplay(description),
        Separator(),
        TextDisplay("**Service Routes**"),
        TextDisplay(
            "**<:dolar:1446871271584567446> Purchase Desk**\n"
            "Quotes · payment methods · order status"
        ),
        TextDisplay(
            "**<:question:1446871332972265512> Support Desk**\n"
            "Troubleshooting · replacements · account help"
        ),
        Separator(),
        TextDisplay("**How It Works**"),
        TextDisplay(
            "1. Pick the route that matches your request.\n"
            "2. Fill in the form with accurate details.\n"
            "3. A specialist joins and resolves it — fully logged."
        ),
        Separator(),
        TextDisplay("**Good To Know**"),
        TextDisplay(
            "• Our team responds in order of arrival — usually within minutes.\n"
            "• Each ticket generates a transcript automatically for your records."
        ),
        Separator(),
    )
    container.color = panel_color
    container.add_item(
        Section(
            TextDisplay("**Ready? Open your ticket below.**"),
            accessory=MakeTicketButton(),
        )
    )
    return container


class TicketPanelView(LayoutView):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
        self.add_item(_build_panel_container(bot))


class TicketPanel(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.panel_store = Path(__file__).with_name("panel.json")
        self.panel_message_id: Optional[int] = None
        self.panel_channel_id: Optional[int] = None

    async def cog_load(self) -> None:
        await self.restore_panel()

    async def restore_panel(self) -> None:
        if not self.panel_store.is_file():
            return
        try:
            with self.panel_store.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
            self.panel_message_id = data.get("message_id")
            self.panel_channel_id = data.get("channel_id")
        except (json.JSONDecodeError, OSError):
            return
        if not self.panel_message_id or not self.panel_channel_id:
            return
        channel = self.bot.get_channel(self.panel_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        try:
            await channel.fetch_message(self.panel_message_id)
        except discord.NotFound:
            return
        view = TicketPanelView(self.bot)
        self.bot.add_view(view, message_id=self.panel_message_id)

    def _save_panel_reference(self, message: discord.Message) -> None:
        payload = {"message_id": message.id, "channel_id": message.channel.id}
        with self.panel_store.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    @commands.command(name="panel")
    @requires_whitelisted()
    async def send_ticket_panel(self, ctx: commands.Context) -> None:
        view = TicketPanelView(self.bot)
        message = await ctx.send(view=view)
        self.bot.add_view(view, message_id=message.id)
        self._save_panel_reference(message)
        logs_channel_id = self.bot.logs_channel_id
        if logs_channel_id:
            log_channel = self.bot.get_channel(logs_channel_id)
            if isinstance(log_channel, discord.TextChannel):
                container = Container(
                    TextDisplay("### Ticket Panel Deployed"),
                    TextDisplay(f"**Sent By:** {ctx.author.mention}"),
                    TextDisplay(f"**Channel:** {ctx.channel.mention}"),
                )
                container.color = discord.Color.green()
                log_view = LayoutView(timeout=None)
                log_view.add_item(container)
                await log_channel.send(view=log_view)


async def setup(bot: commands.Bot):
    if bot.get_cog("TicketPanel") is not None:
        return
    await bot.add_cog(TicketPanel(bot))