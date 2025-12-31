import logging
import io
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch

from app.services.google_sheets import GoogleSheetsService
from app import config

logger = logging.getLogger(__name__)

# Configure matplotlib for non-GUI backend
plt.switch_backend('Agg')

# Set Russian-friendly font
plt.rcParams['font.family'] = 'DejaVu Sans'


class AnalyticsService:
    def __init__(self, gs_service: GoogleSheetsService):
        self.gs_service = gs_service

    def get_transactions_dataframe(self, days: int = 3) -> pd.DataFrame:
        """Fetch transactions and return as DataFrame filtered by last N days."""
        try:
            all_values = self.gs_service.get_all_records(config.FACT_SHEET_NAME)
            if len(all_values) < 2:
                return pd.DataFrame()

            data = all_values[1:]  # Skip header row
            if not data:
                return pd.DataFrame()

            # Create DataFrame without predefined headers
            df = pd.DataFrame(data)

            # Map columns by index (based on actual sheet structure)
            # 0:Date, 1:Category, 2:Subcategory, 3:Amount, 4:Balance, 5:Comment, 6:Currency, 7:Source
            column_mapping = {
                0: 'date',
                1: 'category',
                2: 'subcategory',
                3: 'amount',
                4: 'balance',
                5: 'comment',
                6: 'currency',
                7: 'source'
            }

            # Rename only the columns we need
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k < len(df.columns)})

            # Ensure required columns exist
            required_cols = ['date', 'category', 'subcategory', 'amount']
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"Missing required column: {col}")
                    return pd.DataFrame()

            # Parse dates
            df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y', errors='coerce')

            # Convert amount to float
            # Handle various formats: "63 000" (space as thousands separator), "(650)" (negative in parentheses)
            def parse_amount(val):
                if pd.isna(val) or val == '':
                    return 0.0
                s = str(val).strip()
                # Handle parentheses as negative numbers: (650) -> -650
                is_negative = s.startswith('(') and s.endswith(')')
                if is_negative:
                    s = s[1:-1]
                # Remove spaces and other non-numeric chars except minus and dot/comma
                s = s.replace(' ', '').replace('\u00a0', '')  # regular space and non-breaking space
                s = s.replace(',', '.')  # comma as decimal separator
                try:
                    result = float(s)
                    return -result if is_negative else abs(result)  # abs() for expenses
                except ValueError:
                    return 0.0

            df['amount'] = df['amount'].apply(parse_amount)

            # Filter by date range
            cutoff_date = datetime.now() - timedelta(days=days)
            df = df[df['date'] >= cutoff_date]

            # Exclude internal transfers (СЧЕТА) - these are movements between own accounts
            df = df[~df['category'].str.contains('СЧЕТА', case=False, na=False)]

            return df

        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            return pd.DataFrame()

    def generate_3day_report(self) -> Tuple[str, List[io.BytesIO]]:
        """Generate analytics report for last 3 days with visualizations."""
        df = self.get_transactions_dataframe(days=3)

        if df.empty:
            return "Нет транзакций за последние 3 дня.", []

        charts = []

        # Separate income and expenses
        income_mask = df['category'].str.contains('ДОХОД', case=False, na=False)
        expenses_df = df[~income_mask].copy()
        income_df = df[income_mask].copy()

        # Calculate totals
        total_expenses = expenses_df['amount'].sum()
        total_income = income_df['amount'].sum()
        transaction_count = len(df)

        # Group by category for expenses
        category_totals = expenses_df.groupby('category')['amount'].sum().sort_values(ascending=False)

        # Group by date
        daily_expenses = expenses_df.groupby(expenses_df['date'].dt.date)['amount'].sum()
        daily_income = income_df.groupby(income_df['date'].dt.date)['amount'].sum()

        # Build text report
        report_lines = [
            "📊 *АНАЛИТИКА ЗА 3 ДНЯ*",
            f"📅 Период: {(datetime.now() - timedelta(days=3)).strftime('%d.%m')} - {datetime.now().strftime('%d.%m.%Y')}",
            "",
            "💰 *ИТОГИ:*",
            f"   📈 Доходы: {total_income:,.0f}",
            f"   📉 Расходы: {total_expenses:,.0f}",
            f"   📝 Транзакций: {transaction_count}",
            "",
        ]

        # Top categories
        if not category_totals.empty:
            report_lines.append("📂 *ТОП КАТЕГОРИИ РАСХОДОВ:*")
            for i, (cat, amount) in enumerate(category_totals.head(5).items(), 1):
                pct = (amount / total_expenses * 100) if total_expenses > 0 else 0
                report_lines.append(f"   {i}. {cat}: {amount:,.0f} ({pct:.1f}%)")
            report_lines.append("")

        # Daily breakdown
        report_lines.append("📅 *ПО ДНЯМ:*")
        for i in range(3):
            date = (datetime.now() - timedelta(days=2-i)).date()
            exp = daily_expenses.get(date, 0)
            inc = daily_income.get(date, 0)
            date_str = date.strftime('%d.%m')
            report_lines.append(f"   {date_str}: -{ exp:,.0f} / +{inc:,.0f}")

        # Top transactions
        if not expenses_df.empty:
            report_lines.append("")
            report_lines.append("🔝 *КРУПНЫЕ ТРАТЫ:*")
            top_expenses = expenses_df.nlargest(3, 'amount')
            for _, row in top_expenses.iterrows():
                comment = row.get('comment', '')[:20] if row.get('comment') else ''
                report_lines.append(f"   • {row['amount']:,.0f} - {row['subcategory']} ({comment})")

        report_text = "\n".join(report_lines)

        # Generate charts
        try:
            # Chart 1: Category pie chart
            if not category_totals.empty and len(category_totals) > 0:
                chart1 = self._create_category_pie_chart(category_totals)
                charts.append(chart1)

            # Chart 2: Daily bar chart
            chart2 = self._create_daily_bar_chart(daily_expenses, daily_income)
            charts.append(chart2)

        except Exception as e:
            logger.error(f"Error generating charts: {e}")

        return report_text, charts

    def _create_category_pie_chart(self, category_totals: pd.Series) -> io.BytesIO:
        """Create pie chart for expense categories."""
        fig, ax = plt.subplots(figsize=(10, 8))

        # Limit to top 6 categories, group rest as "Прочее"
        if len(category_totals) > 6:
            top_cats = category_totals.head(6)
            other = category_totals.tail(-6).sum()
            if other > 0:
                top_cats['Другое'] = other
            category_totals = top_cats

        colors = plt.cm.Set3(range(len(category_totals)))

        wedges, texts, autotexts = ax.pie(
            category_totals.values,
            labels=None,
            autopct=lambda pct: f'{pct:.1f}%' if pct > 5 else '',
            colors=colors,
            startangle=90,
            pctdistance=0.75
        )

        # Add legend
        legend_labels = [f'{cat}: {val:,.0f}' for cat, val in category_totals.items()]
        ax.legend(
            wedges, legend_labels,
            title="Категории",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1),
            fontsize=9
        )

        ax.set_title('Расходы по категориям (3 дня)', fontsize=14, fontweight='bold')

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        buf.seek(0)
        plt.close(fig)

        return buf

    def _create_daily_bar_chart(self, daily_expenses: pd.Series, daily_income: pd.Series) -> io.BytesIO:
        """Create bar chart for daily expenses and income."""
        fig, ax = plt.subplots(figsize=(10, 6))

        # Create date range for last 3 days
        dates = [(datetime.now() - timedelta(days=2-i)).date() for i in range(3)]
        date_labels = [d.strftime('%d.%m\n%a') for d in dates]

        # Get values for each date
        expenses = [daily_expenses.get(d, 0) for d in dates]
        income = [daily_income.get(d, 0) for d in dates]

        x = range(len(dates))
        width = 0.35

        bars1 = ax.bar([i - width/2 for i in x], expenses, width, label='Расходы', color='#e74c3c', alpha=0.8)
        bars2 = ax.bar([i + width/2 for i in x], income, width, label='Доходы', color='#27ae60', alpha=0.8)

        # Add value labels on bars
        for bar in bars1:
            height = bar.get_height()
            if height > 0:
                ax.annotate(f'{height:,.0f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=9)

        for bar in bars2:
            height = bar.get_height()
            if height > 0:
                ax.annotate(f'{height:,.0f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=9)

        ax.set_xlabel('Дата')
        ax.set_ylabel('Сумма')
        ax.set_title('Доходы и расходы по дням', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(date_labels)
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        # Format y-axis with thousands separator
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        buf.seek(0)
        plt.close(fig)

        return buf

    def get_summary_stats(self, days: int = 3) -> Dict:
        """Get summary statistics for the period."""
        df = self.get_transactions_dataframe(days=days)

        if df.empty:
            return {}

        income_mask = df['category'].str.contains('ДОХОД', case=False, na=False)
        expenses_df = df[~income_mask]
        income_df = df[income_mask]

        return {
            'total_expenses': expenses_df['amount'].sum(),
            'total_income': income_df['amount'].sum(),
            'transaction_count': len(df),
            'avg_daily_expense': expenses_df['amount'].sum() / days if days > 0 else 0,
            'top_category': expenses_df.groupby('category')['amount'].sum().idxmax() if not expenses_df.empty else None
        }
