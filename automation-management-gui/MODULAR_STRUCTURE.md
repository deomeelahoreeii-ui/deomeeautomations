# Modular GUI structure

The former `complaints_manager/gui/widgets.py` implementation has been divided by feature and responsibility.

- `gui/common/`: shared colors, formatting, desktop actions, and ImGui context helpers.
- `gui/commands/`: generic command list, fields, validation, execution, details, confirmation, and logs.
- `gui/services/`: service list and detail views.
- `gui/crm/`: CRM-specific command actions.
- `gui/sms/`: SMS campaign discovery and screens.
- `gui/whatsapp/`: WhatsApp group selectors and account management screens.
- `gui/antidengue/`: AntiDengue runtime, schedule, repositories, messages, dispatch views, sections, history, and dashboard composition.
- `gui/widgets.py`: compatibility-only re-export facade. New code should import focused modules directly.

The CRM `running`/`command_busy` defect is corrected, and child windows in the main split view and log panels are closed through `try/finally` blocks.
