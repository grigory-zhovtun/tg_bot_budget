import google.generativeai as genai
import logging
import json
from typing import List, Dict, Optional, Any
from app import config
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

class GeminiService:
    def __init__(self, gs_service: GoogleSheetsService):
        self.gs_service = gs_service
        self._setup_client()

    def _setup_client(self):
        if config.GEMINI_API_KEY:
            genai.configure(api_key=config.GEMINI_API_KEY)
            
            # Debug: List available models
            try:
                logger.info("Listing available Gemini models:")
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        logger.info(f"- {m.name}")
            except Exception as e:
                logger.error(f"Failed to list models: {e}")

            # Fallback to stable 'gemini-pro' if 1.5 is having issues, or we can try to pick from list.
            # Based on logs, 'gemini-flash-latest' is available and safe.
            self.model = genai.GenerativeModel('gemini-flash-latest') 
            logger.info("Gemini AI client configured with 'gemini-flash-latest'.")
        else:
            logger.warning("GEMINI_API_KEY not found. AI features will be disabled.")
            self.model = None

    def get_history_context(self, limit: int = 50) -> str:
        """
        Fetches recent transactions from Google Sheets to use as few-shot examples.
        Returns a formatted string for the prompt.
        """
        try:
            # We need to access the sheet directly. 
            # Assuming gs_service has a method or we access client.
            # Ideally GS service should expose 'get_recent_rows'.
            # For now, we will use the internal sheet object if available or add a method to GS service.
            # Let's add a method to GS service or just use private access if we must (not ideal).
            # Better: use gs_service.sheet.worksheet(config.FACT_SHEET_NAME).get_all_values()
            # But that pulls ALL rows. Inefficient for large history.
            # We'll do it for now or assume recent rows.
            
            if not self.gs_service.sheet:
                self.gs_service._authenticate()
                
            fact_sheet = self.gs_service.sheet.worksheet(config.FACT_SHEET_NAME)
            
            # Optimization: get last N rows. 
            # GSpread doesn't have "tail", so we get total count then get range.
            total_rows = len(fact_sheet.get_all_values()) # Expensive call unfortunately, unless we use something else.
            # Actually, we can assume the user has < 10k rows? 
            # Or just fetch all values. 
            
            all_values = fact_sheet.get_all_values()
            headers = all_values[0]
            # Assuming standard columns: Date, Category, Subcategory, Amount, ..., Comment, ...
            # We want Date, Category, Subcat, Amount, Comment, Source
            
            # Take last 'limit' rows
            recent_rows = all_values[-limit:] if len(all_values) > limit else all_values[1:]
            
            context_str = "History of recent user transactions (Format: Date | Category | Subcategory | Amount | Comment | Source):\n"
            for row in recent_rows:
                # Safe index access
                try:
                    # Adjust indices based on your sheet structure:
                    # 0:Date, 1:Cat, 2:Sub, 3:Amt, 5:Comment, 7:Source (based on bot.py)
                    d = row[0]
                    c = row[1]
                    s = row[2]
                    a = row[3]
                    cmt = row[5] if len(row) > 5 else ""
                    src = row[7] if len(row) > 7 else ""
                    context_str += f"{d} | {c} | {s} | {a} | {cmt} | {src}\n"
                except IndexError:
                    continue
            
            return context_str
            
        except Exception as e:
            logger.error(f"Failed to fetch history context: {e}")
            return "No history available."

    async def parse_transaction(self, user_input: str, image_part: Any = None, known_categories: List[str] = [], known_sources: List[str] = []) -> Dict[str, Any]:
        """
        Parses text or image input using Gemini to extract transaction details.
        """
        if not self.model:
            raise ValueError("AI Service not configured (missing API Key).")

        history_context = self.get_history_context(limit=30)
        
        cats_str = ", ".join(known_categories)
        sources_str = ", ".join(known_sources)

        prompt_parts = [
            f"You are a personal finance assistant. Analyze the input and extract transaction details.",
            f"Allowed Categories: {cats_str}",
            f"Allowed Sources: {sources_str}",
            f"\nCONTEXT (User's habits):\n{history_context}\n",
            f"INSTRUCTION:",
            f"1. Extract: Amount (float), Currency (ISO code if found, else null), Date (DD.MM.YYYY), Category, Subcategory, Comment, Source.",
            f"2. Use the History Context to predict the Category and Subcategory based on the Comment/Merchant name.",
            f"3. If Source is not explicitly mentioned in input, try to infer it from context, otherwise return null.",
            f"4. If exact Date is not in input, use today's date.",
            f"5. Return ONLY valid JSON. No markdown formatting.",
            f"JSON Schema: {{'amount': float, 'currency': str, 'date': str, 'category': str, 'subcategory': str, 'comment': str, 'source': str}}",
            f"\nINPUT: {user_input}"
        ]
        
        content = prompt_parts
        if image_part:
            content.append(image_part)

        try:
            response = self.model.generate_content(content)
            text_resp = response.text.replace("```json", "").replace("```", "").strip()
            data = json.loads(text_resp)
            return data
        except Exception as e:
            logger.error(f"Gemini API Error: {e}")
            # Identify if it was a safety block or parsing error
            if "safety" in str(e).lower():
                raise ValueError("AI blocked the content for safety reasons.")
            raise ValueError(f"Could not understand transaction: {e}")
