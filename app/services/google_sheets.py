import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import List, Dict, Optional, Tuple, Any
from app import config

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    def __init__(self):
        self.client: Optional[gspread.Client] = None
        self.sheet: Optional[gspread.Spreadsheet] = None
        self._authenticate()

    def _authenticate(self):
        """Authenticates with Google Sheets API."""
        try:
            if config.GOOGLE_APPLICATION_CREDENTIALS_PATH and os.path.exists(config.GOOGLE_APPLICATION_CREDENTIALS_PATH):
                self.client = gspread.service_account(filename=config.GOOGLE_APPLICATION_CREDENTIALS_PATH)
            
            elif config.GOOGLE_SERVICE_ACCOUNT_EMAIL and config.GOOGLE_PRIVATE_KEY:
                creds_info = {
                    "type": "service_account",
                    "private_key": config.GOOGLE_PRIVATE_KEY,
                    "client_email": config.GOOGLE_SERVICE_ACCOUNT_EMAIL,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{config.GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"
                }
                creds = Credentials.from_service_account_info(
                    creds_info, 
                    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
                )
                self.client = gspread.authorize(creds)
            else:
                # Fallback to default auth if available (e.g. cloud environment)
                import google.auth
                scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
                creds, _ = google.auth.default(scopes=scopes)
                self.client = gspread.authorize(creds)

            self.sheet = self.client.open_by_key(config.SPREADSHEET_ID)
            logger.info("Successfully connected to Google Sheets.")

        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets: {e}")
            raise

    def get_categories_and_sources(self) -> Tuple[List[str], Dict[str, List[str]], List[str]]:
        """Retrieves categories, subcategories, and sources from the 'system' sheet."""
        if not self.sheet:
            self._authenticate()
        
        try:
            system_sheet = self.sheet.worksheet(config.SYSTEM_SHEET_NAME)
            data = system_sheet.get_all_values()
        except Exception as e:
            logger.error(f"Error reading system sheet: {e}")
            # Simple retry mechanism could be added here
            self._authenticate()
            system_sheet = self.sheet.worksheet(config.SYSTEM_SHEET_NAME)
            data = system_sheet.get_all_values()

        categories = []
        subcategories = {}
        sources = []

        if len(data) > 1:
            for row in data[1:]:  # Skip header
                # Ensure row has enough columns
                row += [""] * (6 - len(row))
                
                cat = row[0].strip()
                sub = row[1].strip()
                src = row[5].strip()

                if cat and cat not in categories:
                    categories.append(cat)
                
                if cat and sub:
                    subcategories.setdefault(cat, []).append(sub)
                
                if src and src not in sources:
                    sources.append(src)
        
        return categories, subcategories, sources

    def add_transaction(self, row_data: List[Any]) -> bool:
        """Appends a transaction row to the 'fact' sheet."""
        if not self.sheet:
            self._authenticate()

        try:
            fact_sheet = self.sheet.worksheet(config.FACT_SHEET_NAME)
            fact_sheet.append_row(row_data, value_input_option='USER_ENTERED')
            return True
        except Exception as e:
            logger.error(f"Error appending transaction: {e}")
            # Try to reconnect and retry once
            try:
                self._authenticate()
                fact_sheet = self.sheet.worksheet(config.FACT_SHEET_NAME)
                fact_sheet.append_row(row_data, value_input_option='USER_ENTERED')
                return True
            except Exception as retry_e:
                logger.error(f"Retry failed: {retry_e}")
                return False

            return len(fact_sheet.get_all_values())
        except Exception as e:
            logger.error(f"Error getting last row index: {e}")
            return 0

    def get_all_records(self, worksheet_name: str) -> List[List[Any]]:
        """Retrieves all records from a worksheet with retry logic."""
        if not self.sheet:
            self._authenticate()
        
        retries = 3
        for attempt in range(retries):
            try:
                # Re-auth if client session expired
                if not self.sheet:
                    self._authenticate()

                ws = self.sheet.worksheet(worksheet_name)
                return ws.get_all_values()
                
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{retries} failed to fetch records from {worksheet_name}: {e}")
                # 401/403 -> Force refresh auth
                if "401" in str(e) or "403" in str(e) or "Connection aborted" in str(e):
                    logger.info("Forcing re-authentication...")
                    try:
                        self._authenticate()
                    except Exception as auth_e:
                        logger.error(f"Re-auth failed: {auth_e}")
                
                if attempt == retries - 1:
                    logger.error(f"Final failure fetching records: {e}")
                    raise e
        return []
