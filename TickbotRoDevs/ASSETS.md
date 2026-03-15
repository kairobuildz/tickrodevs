# Skyre's MP Asset Map

This project no longer hardcodes embed images. Instead, brand styling is driven by configuration so you can drop in new assets without touching code.

- `config.json`
  - `brand_name`, `activity_text`: controls the name shown in the panel container headlines and the Discord presence.
  - `ticket_panel_color`, `website_container_color`, `tos_container_color`: hex colors for the container headers.
  - `website_url`, `tos_url`: destination URLs for the website and ToS buttons.
  - `assets`: stores logos, banner art, and button emojis. Update `primary_logo` and `panel_banner` to swap imagery, and `purchase_icon` / `support_icon` to change the panel buttons.
- `data/ticket_owners.json` (generated): persistent map of ticket channel IDs to the original requester IDs.
- `data/transcripts/` (generated): transcripts are written here briefly before being dispatched and deleted.

If you decide to reintroduce static images (logos, banners, etc.), place them inside an `assets/` folder and update the relevant config field or container text to reference the new path or CDN URL. Keep this file up to date so the team always knows where branding resources live.
