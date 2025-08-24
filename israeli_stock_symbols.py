import pandas as pd
import requests
import time
from typing import Optional, Dict, List
import yfinance as yf
from fuzzywuzzy import fuzz

class IsraeliStockSymbolFinder:
    def __init__(self):
        self.session = requests.Session()
        # רשימה מורחבת של מניות ישראליות נפוצות לחיפוש מהיר
        self.known_israeli_stocks = {
            'בנק הפועלים': 'POLI.TA',
            'פועלים': 'POLI.TA',
            'בנק לאומי': 'LUMI.TA',
            'לאומי': 'LUMI.TA',
            'בזק': 'BEZQ.TA',
            'טבע': 'TEVA.TA',
            'אלביט': 'ESLT.TA',
            'נייס': 'NICE.TA',
            'צ\'ק פוינט': 'CHKP.TA',
            'פרטנר': 'PTNR.TA',
            'דלק קבוצה': 'DLEKG.TA',
            'דלק': 'DLEKG.TA',
            'אמות': 'AMOT.TA',
            'אלקו': 'ALCO.TA',
            'דיסקונט': 'DSCT.TA',
            'מזרחי טפחות': 'MZTF.TA',
            'הפניקס': 'PHNX.TA',
            'הראל השקעות': 'HARL.TA',
            'מגדל ביטוח': 'MGDL.TA',
            'שופרסל': 'SAE.TA',
            'שטראוס גרופ': 'STRS.TA',
            'שיכון ובינוי': 'SKBN.TA',
            'איילון': 'AYLN.TA',
            'אזורים': 'AZRM.TA',
            'ישראכרט': 'ISCD.TA',
            'ישרס': 'ISRS.TA',
            'סלקום': 'CEL.TA',
            'בינלאומי': 'BINT.TA',
            'אלקטרה': 'ELTR.TA',
            'אינטרקיור': 'INCR.TA',
            'דניה סיבוס': 'DNYA.TA',
            'רמי לוי': 'RMLI.TA',
            'אפקון החזקות': 'AFKN.TA',
            'פסגות קבוצה': 'PSGM.TA',
            'מיטב בית השקעות': 'MTDS.TA',
            'מנורה מב החזקות': 'MNRB.TA',
            'ברקת קפיטל': 'BRKT.TA',
            'פלאזה סנטרס': 'PLAZ.TA',
            'קנדה ישראל': 'CLIS.TA',
            'ישרוטל': 'ISHT.TA',
            'אלוני חץ': 'ALHE.TA',
            'תדיראן גרופ': 'TADR.TA',
            'מלם תים': 'MLTM.TA',
            'אלביט טכנ': 'ESLT.TA',
            'אלביט הדמיה': 'ELMD.TA',
            'אורביט': 'ORBT.TA',
            'אליביט': 'ESLT.TA'
        }
    
    def search_symbol_yfinance(self, company_name: str) -> Optional[str]:
        """חיפוש סמל מניה באמצעות yfinance"""
        try:
            # ניסוי לחפש עם .TA (תל אביב)
            possible_symbols = [
                f"{company_name.upper()}.TA",
                f"{company_name.replace(' ', '').upper()}.TA",
                company_name.upper(),
            ]
            
            for symbol in possible_symbols:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    if info and 'symbol' in info:
                        return symbol
                except:
                    continue
            
            return None
        except Exception as e:
            print(f"שגיאה בחיפוש {company_name}: {e}")
            return None
    
    def fuzzy_search_known_stocks(self, company_name: str, threshold: int = 80) -> Optional[str]:
        """חיפוש מטושטש במניות הידועות"""
        best_match = None
        best_score = 0
        
        for known_name, symbol in self.known_israeli_stocks.items():
            score = fuzz.partial_ratio(company_name.lower(), known_name.lower())
            if score > best_score and score >= threshold:
                best_score = score
                best_match = symbol
        
        return best_match
    
    def search_symbol_alpha_vantage(self, company_name: str, api_key: str) -> Optional[str]:
        """חיפוש באמצעות Alpha Vantage API (דורש מפתח API)"""
        try:
            url = f"https://www.alphavantage.co/query"
            params = {
                'function': 'SYMBOL_SEARCH',
                'keywords': company_name,
                'apikey': api_key
            }
            
            response = self.session.get(url, params=params)
            data = response.json()
            
            if 'bestMatches' in data and data['bestMatches']:
                for match in data['bestMatches']:
                    symbol = match.get('1. symbol', '')
                    if '.TA' in symbol or 'TLV' in symbol:
                        return symbol
            
            return None
        except Exception as e:
            print(f"שגיאה ב-Alpha Vantage עבור {company_name}: {e}")
            return None
    
    def add_symbols_to_dataframe(self, df: pd.DataFrame, 
                                 company_column: str, 
                                 alpha_vantage_key: str = None) -> pd.DataFrame:
        """הוספת סמלי מניות לטבלה"""
        df_copy = df.copy()
        df_copy['Symbol'] = None
        df_copy['Search_Method'] = None
        
        for idx, row in df_copy.iterrows():
            company_name = str(row[company_column])
            symbol = None
            method = None
            
            print(f"מחפש סמל עבור: {company_name}")
            
            # 1. חיפוש במניות הידועות
            symbol = self.fuzzy_search_known_stocks(company_name)
            if symbol:
                method = "Known Stock"
            
            # 2. אם לא נמצא, חיפוש ב-yfinance
            if not symbol:
                symbol = self.search_symbol_yfinance(company_name)
                if symbol:
                    method = "YFinance"
            
            # 3. אם יש מפתח Alpha Vantage, נסה גם שם
            if not symbol and alpha_vantage_key:
                symbol = self.search_symbol_alpha_vantage(company_name, alpha_vantage_key)
                if symbol:
                    method = "Alpha Vantage"
            
            df_copy.at[idx, 'Symbol'] = symbol if symbol else "לא נמצא"
            df_copy.at[idx, 'Search_Method'] = method if method else "לא נמצא"
            
            # השהייה קטנה כדי לא לעמוס על ה-APIs
            time.sleep(0.5)
        
        return df_copy

# פונקציה להעלת הקובץ שלך ועיבוד
def process_user_stocks():
    """עיבוד קובץ המניות של המשתמש"""
    
    # קריאת הקובץ
    try:
        df = pd.read_csv('מניות מותרות.csv', encoding='utf-8-sig')
        print(f"נטענו {len(df)} מניות מהקובץ")
    except:
        # אם יש בעיה עם הקידוד
        df = pd.read_csv('מניות מותרות.csv', encoding='cp1255')
        print(f"נטענו {len(df)} מניות מהקובץ")
    
    print("המניות הראשונות בקובץ:")
    print(df.head(10))
    print("\n" + "="*50 + "\n")
    
    # יצירת אובייקט החיפוש
    finder = IsraeliStockSymbolFinder()
    
    # הוספת סמלים - השם של העמודה הוא 'שם'
    print("מתחיל חיפוש סמלים...")
    df_with_symbols = finder.add_symbols_to_dataframe(df, 'שם')
    
    print("\n" + "="*50)
    print("סיכום התוצאות:")
    print("="*50)
    
    # סיכום התוצאות
    found_count = len(df_with_symbols[df_with_symbols['Symbol'] != 'לא נמצא'])
    not_found_count = len(df_with_symbols[df_with_symbols['Symbol'] == 'לא נמצא'])
    
    print(f"סמלים שנמצאו: {found_count}")
    print(f"סמלים שלא נמצאו: {not_found_count}")
    print(f"אחוז הצלחה: {(found_count/len(df)*100):.1f}%")
    
    # הצגת דוגמאות למניות שנמצאו
    found_stocks = df_with_symbols[df_with_symbols['Symbol'] != 'לא נמצא']
    if len(found_stocks) > 0:
        print("\nדוגמאות למניות שנמצאו:")
        print(found_stocks[['שם', 'Symbol', 'Search_Method']].head(10))
    
    # הצגת מניות שלא נמצאו
    not_found_stocks = df_with_symbols[df_with_symbols['Symbol'] == 'לא נמצא']
    if len(not_found_stocks) > 0:
        print("\nמניות שלא נמצא להן סמל (דוגמאות):")
        print(not_found_stocks['שם'].head(10).tolist())
    
    # שמירת התוצאה
    output_filename = 'israeli_stocks_with_symbols_complete.csv'
    df_with_symbols.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"\nהקובץ המלא נשמר בשם: {output_filename}")
    
    return df_with_symbols

# הוראות התקנת חבילות נדרשות
def install_requirements():
    """הדפסת הוראות התקנה"""
    requirements = [
        "pandas",
        "requests", 
        "yfinance",
        "fuzzywuzzy[speedup]"
    ]
    
    print("כדי להשתמש בסקריפט, התקן את החבילות הבאות:")
    print("pip install " + " ".join(requirements))

if __name__ == "__main__":
    print("Israeli Stock Symbol Finder - מחפש סמלים עבור קובץ המניות שלך")
    print("="*60)
    
    # הדפסת הוראות התקנה
    install_requirements()
    print("\n")
    
    # עיבוד הקובץ שלך
    try:
        result_df = process_user_stocks()
        print("\nהסקריפט הושלם בהצלחה!")
        print("בדוק את הקובץ: israeli_stocks_with_symbols_complete.csv")
    except ImportError as e:
        print(f"שגיאה: חסרה חבילה. {e}")
        print("אנא התקן את החבילות הנדרשות.")
    except FileNotFoundError:
        print("שגיאה: לא נמצא הקובץ 'מניות מותרות.csv'")
        print("וודא שהקובץ נמצא באותה תיקייה של הסקריפט.")
    except Exception as e:
        print(f"שגיאה כללית: {e}")
        print("ניתן לנסות להשתמש בפונקציה example_usage() לבדיקה.")
