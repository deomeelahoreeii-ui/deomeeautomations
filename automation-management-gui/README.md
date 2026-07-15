# Automation Management GUI

Standalone desktop console for the sibling CRM, PMDU, and AntiDengue automation
systems.

It also exposes the sibling `../add-to-whatsapp-group` importer. That screen can
preview an Excel selection safely, or start NATS and the WhatsApp worker before
queuing confirmed group-membership changes.

Run it from this folder:

```bash
/home/ahmad/.local/bin/uv run python main.py
```

Expected sibling folders:

- `../crm-management-system`
- `../pmdu-management-system`
- `../antidengue`
- `../add-to-whatsapp-group`

The GUI stores its own settings in `.complaints_manager.json`. Commands still run
inside the relevant system folder, so each system keeps its own `.env`, data,
artifacts, and virtual environment.
