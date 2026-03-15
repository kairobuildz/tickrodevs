from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands
from discord.ui import Button, Container, LayoutView, Section, Select, Separator, TextDisplay, View

from commands.tickets.panel import requires_whitelisted
from commands.tickets.create import parse_int


APPLICATION_STORE_NAME = "application_panel.json"
APPLICATION_UPDATES_LINK = "https://discord.com/channels/1436694663879983146/1436694664840220812"


class ApplicationSelect(Select):
    def __init__(self, manager: "TicketManager", requester: discord.User):
        options = [
            discord.SelectOption(
                label="Report's Moderator",
                value="Report's Moderator",
                emoji="<:orange:1445432929101680660>",
            ),
            discord.SelectOption(
                label="Support's Moderator",
                value="Support's Moderator",
                emoji="<:yellow:1445432958784639108>",
            ),
            discord.SelectOption(
                label="Advertising's Team",
                value="Advertising's Team",
                emoji="<:purple:1445432866925187273>",
            ),
        ]
        super().__init__(
            placeholder="Which Kind of Moderator?",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="application:select",
        )
        self.manager = manager
        self.requester = requester

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message("Use your own selector to submit an application.", ephemeral=True)
            return
        manager = self.manager
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            pass
        try:
            confirmation = await manager._create_ticket(
                interaction,
                "application",
                {"application_role": f"```\n{self.values[0]}\n```"},
            )
        except Exception as exc:
            await interaction.followup.send(f"<:no:1446871285438349354> Could not open your application: `{exc}`", ephemeral=True)
            return
        await interaction.followup.send(confirmation, ephemeral=True)


class ApplicationSelectView(View):
    def __init__(self, manager: "TicketManager", requester: discord.User):
        super().__init__(timeout=120)
        self.add_item(ApplicationSelect(manager, requester))


class ApplicationOpenButton(Button):
    def __init__(self, is_open: bool):
        label = "Applications Are Currently Open!" if is_open else "Applications Are Currently Closed!"
        style = discord.ButtonStyle.success if is_open else discord.ButtonStyle.danger
        super().__init__(
            label=label,
            style=style,
            emoji="<:member:1445407110270816328>",
            custom_id="application:open-button",
        )
        self.is_open = is_open

    async def callback(self, interaction: discord.Interaction) -> None:
        manager = interaction.client.get_cog("TicketManager")
        if manager is None:
            await interaction.response.send_message("Ticket system is not ready yet. Please try again shortly.", ephemeral=True)
            return
        if not self.is_open:
            await interaction.response.send_message(
                f"Applications are currently closed! Keep track of: {APPLICATION_UPDATES_LINK} For Any Updates!",
                ephemeral=True,
            )
            return
        selector_view = ApplicationSelectView(manager, interaction.user)
        await interaction.response.send_message(
            "Select the moderator role you want to apply for:",
            view=selector_view,
            ephemeral=True,
        )


class ApplicationPanelView(LayoutView):
    def __init__(self, bot: commands.Bot, *, is_open: bool):
        super().__init__(timeout=None)
        brand_name = getattr(bot, "brand_name", "Kairo's Studio")
        color_value = parse_int(bot.config.get("ticket_panel_color"), 0x5865F2)
        color = discord.Color(color_value)
        container = Container(
            TextDisplay(f"### 🎫 {brand_name} Applications"),
            TextDisplay("Apply for a moderator position below."),
            Separator(),
            TextDisplay("**Positions**"),
            TextDisplay(
                "- Report's Moderator\n"
                "- Support's Moderator\n"
                "- Advertising's Team"
            ),
            Separator(),
            TextDisplay("**How To Apply**"),
            TextDisplay(
                "Click the button below and choose the position you want. "
                "A ticket will open with the application format."
            ),
        )
        container.color = color
        container.add_item(Separator())
        container.add_item(
            Section(
                TextDisplay("Application status"),
                accessory=ApplicationOpenButton(is_open),
            )
        )
        self.add_item(container)


class ApplicationPanel(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.store_path = Path(bot.data_dir) / APPLICATION_STORE_NAME
        self.panel_message_id: Optional[int] = None
        self.panel_channel_id: Optional[int] = None
        self.is_open: bool = True

    async def cog_load(self) -> None:
        self._load_state()
        await self._restore_panel()

    def _load_state(self) -> None:
        if not self.store_path.is_file():
            return
        try:
            with self.store_path.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
            self.panel_message_id = data.get("message_id")
            self.panel_channel_id = data.get("channel_id")
            self.is_open = bool(data.get("is_open", True))
        except (json.JSONDecodeError, OSError):
            return

    def _save_state(self) -> None:
        payload = {
            "message_id": self.panel_message_id,
            "channel_id": self.panel_channel_id,
            "is_open": self.is_open,
        }
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with self.store_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    async def _restore_panel(self) -> None:
        if not self.panel_channel_id or not self.panel_message_id:
            return
        channel = self.bot.get_channel(self.panel_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        try:
            await channel.fetch_message(self.panel_message_id)
        except discord.NotFound:
            return
        view = ApplicationPanelView(self.bot, is_open=self.is_open)
        self.bot.add_view(view, message_id=self.panel_message_id)

    async def _send_panel(self, channel: discord.TextChannel) -> None:
        view = ApplicationPanelView(self.bot, is_open=self.is_open)
        message = await channel.send(view=view)
        self.bot.add_view(view, message_id=message.id)
        self.panel_channel_id = channel.id
        self.panel_message_id = message.id
        self._save_state()

    async def _update_panel(self, ctx: commands.Context) -> None:
        if not self.panel_channel_id or not self.panel_message_id:
            await ctx.send("No application panel to update.")
            return
        channel = self.bot.get_channel(self.panel_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await ctx.send("Stored application panel channel is missing.")
            return
        try:
            message = await channel.fetch_message(self.panel_message_id)
        except discord.NotFound:
            await ctx.send("Stored application panel message is missing.")
            return
        view = ApplicationPanelView(self.bot, is_open=self.is_open)
        await message.edit(view=view)
        self.bot.add_view(view, message_id=message.id)
        self._save_state()
        await ctx.send(f"Application panel updated. Status: {'open' if self.is_open else 'closed'}.")

    @commands.command(name="application")
    @requires_whitelisted()
    async def send_application_panel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None) -> None:
        target_channel = channel or (ctx.channel if isinstance(ctx.channel, discord.TextChannel) else None)
        if target_channel is None:
            await ctx.send("Please specify a text channel to send the application panel.")
            return
        self.panel_channel_id = target_channel.id
        self.panel_message_id = None
        await self._send_panel(target_channel)
        await ctx.send(f"Application panel sent to {target_channel.mention}.")

    @commands.command(name="appopen")
    @requires_whitelisted()
    async def open_applications(self, ctx: commands.Context) -> None:
        self.is_open = True
        await self._update_panel(ctx)

    @commands.command(name="appclose")
    @requires_whitelisted()
    async def close_applications(self, ctx: commands.Context) -> None:
        self.is_open = False
        await self._update_panel(ctx)


async def setup(bot: commands.Bot):
    if bot.get_cog("ApplicationPanel") is not None:
        return
    await bot.add_cog(ApplicationPanel(bot))
