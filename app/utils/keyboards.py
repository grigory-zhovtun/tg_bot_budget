from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from typing import List, Dict, Optional

def generate_categories_keyboard(categories: List[str]) -> InlineKeyboardMarkup:
    """Generates an inline keyboard for category selection."""
    keyboard = []
    row_buttons = []
    for idx, category in enumerate(categories, 1):
        row_buttons.append(InlineKeyboardButton(text=category, callback_data=f"cat_{category}"))
        if idx % 3 == 0 or idx == len(categories):
            keyboard.append(row_buttons)
            row_buttons = []
    
    # Ensure any remaining buttons are added
    if row_buttons:
        keyboard.append(row_buttons)

    # Action buttons (e.g. SMS replaced by AI-prompts implicitly, but keeping generic structure)
    # The user wanted AI everywhere, so maybe "SMS/Text" is just the default mode?
    # Keeping the original button structure for now to maintain familiarity, but "SMS" might be redundant if text input works.
    # However, the original code had a specific "SMS" button to switch mode. 
    # With AI, we might not need a specific mode, but for now let's keep the UI consistent or simplify.
    # The requirement was "AI can determine...".
    # I will keep the "SMS" button for now but it might trigger the AI hint or mode.
    # Wait, the plan says AI-first. So maybe we don't need an explicit "SMS" button if ANY text is treated as a transaction?
    # But for manual entry flow (Source -> Cat -> Subcat), the user is in a state machine.
    # If they want to paste an SMS, they might need to exit that state?
    # Let's keep the "manual" flow as is, and "SMS" button just tells them "Paste it now".
    
    # Original:
    # action_buttons_row = [InlineKeyboardButton(text="СМС", callback_data="sms")]
    # keyboard.append(action_buttons_row)
    
    return InlineKeyboardMarkup(keyboard)

def generate_sources_keyboard(sources: List[str], current_source: Optional[str] = None) -> ReplyKeyboardMarkup:
    """Generates a reply keyboard for source selection."""
    buttons = []
    if not sources:
        buttons = [["Нет доступных источников"]]
    else:
        for i in range(0, len(sources), 3):
            row = []
            for source in sources[i:i+3]:
                if source == current_source:
                    row.append(f"✅ {source}")
                else:
                    row.append(source)
            buttons.append(row)
    
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def generate_subcategories_keyboard(
    subcategories: Dict[str, List[str]], 
    selected_category: str
) -> InlineKeyboardMarkup:
    """Generates an inline keyboard for subcategory selection."""
    keyboard = []
    row_buttons = []
    subs = subcategories.get(selected_category, [])
    
    for idx, sub_name in enumerate(subs, 1):
        row_buttons.append(InlineKeyboardButton(text=sub_name, callback_data=f"sub_{sub_name}"))
        if idx % 2 == 0 or idx == len(subs):
            keyboard.append(row_buttons)
            row_buttons = []
    
    if row_buttons:
        keyboard.append(row_buttons)
        
    keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_categories")
    ])
    return InlineKeyboardMarkup(keyboard)
