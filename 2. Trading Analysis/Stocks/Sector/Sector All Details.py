import requests
import json
import pandas as pd
from urllib.parse import quote

BASE = "https://iislliveblob.niftyindices.com/jsonfiles"

CATEGORIES = {
    "MacroEconomicSector": "MacroEconomicSector",
    "Sector": "Sector",
    "Industry": "Industry",
    "Basic Industry": "BasicIndustry"
}

# ==========================================================
def fetch_category(index_name, category):
    folder = quote(category)
    index  = quote(index_name)
    
    url = f"{BASE}/{folder}/SectorialIndexData{index}_{category.replace(' ', '')}.js"
    print(f"Fetching → {category}")
    
    text = requests.get(url, timeout=15).text

    start = text.find("{")
    depth = 0
    for i, ch in enumerate(text[start:], start=start):
        if ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    data = json.loads(text[start:end].replace(",]", "]").replace(",}", "}"))

    rows = []
    for grp in data["groups"]:
        group_name   = " ".join(grp["label"].split()[:-1])
        group_weight = grp.get("weight", 0)

        for stk in grp["groups"]:
            stock = stk["label"].split()[0]
            stock_weight = stk.get("weight", 0)

            rows.append({
                "Stock": stock,
                CATEGORIES[category]: group_name,
                f"{CATEGORIES[category]}Weight%": group_weight,
                f"{CATEGORIES[category]}StockWeight%": stock_weight
            })

    return pd.DataFrame(rows)

# ==========================================================
def Build_Economic_Map(index_name):

    dfs = {cat: fetch_category(index_name, cat) for cat in CATEGORIES}

    master = (
        dfs["MacroEconomicSector"]
        .merge(dfs["Sector"], on="Stock", how="left")
        .merge(dfs["Industry"], on="Stock", how="left")
        .merge(dfs["Basic Industry"], on="Stock", how="left")
    )

    # Final weights
    master["StockWeight%"]  = master["MacroEconomicSectorStockWeight%"]
    master["SectorWeight%"] = master["SectorWeight%"]

    # -------------------------------
    # GUARANTEED BasicIndustryWeight%
    # -------------------------------
    master["BasicIndustryWeight%"] = (
        master.groupby(["Sector","BasicIndustry"])["StockWeight%"]
        .transform("sum")
    )

    # Final order
    master = master[[
        "MacroEconomicSector",
        "Sector",
        "Industry",
        "BasicIndustry",
        "Stock",
        "SectorWeight%",
        "StockWeight%",
        "BasicIndustryWeight%"
    ]]

    # Sort
    master = master.sort_values(
        by=["SectorWeight%","BasicIndustryWeight%","StockWeight%"],
        ascending=[False,False,False]
    )

    # Excel-style hierarchy
    cols = ["MacroEconomicSector","Sector","Industry","BasicIndustry"]
    master[cols] = master[cols].where(master[cols].ne(master[cols].shift()))

    file = f"{index_name.replace(' ', '_')}_Economic_Exposure.xlsx"
    master.to_excel(file, index=False)
    print(f"\nSaved → {file}")

    return master

# ==========================================================
if __name__ == "__main__":
    df = Build_Economic_Map("NIFTY 500")
    print(df.head(10))


#---------------------------------------------------------------------------------------------------------------------------------
'''
if you want .csv file & All Cell All Data need use below code
'''

# import requests
# import json
# import pandas as pd
# from urllib.parse import quote

# # ==========================================================
# # Constants
# # ==========================================================
# BASE = "https://iislliveblob.niftyindices.com/jsonfiles"

# CATEGORIES = {
#     "MacroEconomicSector": "MacroEconomicSector",
#     "Sector": "Sector",
#     "Industry": "Industry",
#     "Basic Industry": "BasicIndustry"
# }

# # ==========================================================
# # Download & Normalize One NSE Category with WEIGHTS
# # ==========================================================
# def fetch_category(index_name, category):
#     folder = quote(category)
#     index  = quote(index_name)
    
#     url = f"{BASE}/{folder}/SectorialIndexData{index}_{category.replace(' ', '')}.js"
#     print(f"Fetching → {category}")
    
#     text = requests.get(url, timeout=15).text

#     # ---- Extract valid JSON from JS
#     start = text.find("{")
#     depth = 0
#     for i, ch in enumerate(text[start:], start=start):
#         if ch == "{": depth += 1
#         elif ch == "}":
#             depth -= 1
#             if depth == 0:
#                 end = i + 1
#                 break

#     data = json.loads(text[start:end].replace(",]", "]").replace(",}", "}"))

#     rows = []
#     for grp in data["groups"]:
#         group_name   = " ".join(grp["label"].split()[:-1])
#         group_weight = grp.get("weight", 0)

#         for stk in grp["groups"]:
#             stock = stk["label"].split()[0]
#             stock_weight = stk.get("weight", 0)

#             rows.append({
#                 "Stock": stock,
#                 CATEGORIES[category]: group_name,
#                 f"{CATEGORIES[category]}Weight%": group_weight,
#                 f"{CATEGORIES[category]}StockWeight%": stock_weight
#             })

#     return pd.DataFrame(rows)


# # ==========================================================
# # Build FULL Institutional Economic Exposure Map
# # ==========================================================
# def Build_Economic_Map(index_name):
#     # --- Fetch all category data
#     dfs = {cat: fetch_category(index_name, cat) for cat in CATEGORIES}

#     # --- Merge all on 'Stock'
#     master = dfs["MacroEconomicSector"] \
#         .merge(dfs["Sector"], on="Stock", how="left") \
#         .merge(dfs["Industry"], on="Stock", how="left") \
#         .merge(dfs["Basic Industry"], on="Stock", how="left")

#     # --- Final stock-level weight
#     master["StockWeight%"] = master["MacroEconomicSectorStockWeight%"]

#     # --- Sector weight
#     master["SectorWeight%"] = master["SectorWeight%"]

#     # --- Clean columns (your original format)
#     master = master[["MacroEconomicSector", "Sector", "Industry", "BasicIndustry", "Stock", "SectorWeight%", "StockWeight%"]]

#     # ======================================================
#     # 1. Compute BasicIndustry total weight per Sector
#     # ======================================================
#     bi_weight = master.groupby(["Sector", "BasicIndustry"])["StockWeight%"].sum().reset_index()

#     bi_weight = bi_weight.rename(columns={"StockWeight%": "BasicIndustryWeight%"})

#     master = master.merge(bi_weight, on=["Sector", "BasicIndustry"], how="left")

#     # ======================================================
#     # 2. TRUE 3-LEVEL CAPITAL SORT
#     # ======================================================
#     master = master.sort_values(by=["SectorWeight%", "BasicIndustryWeight%", "StockWeight%"], ascending=[False, False, False])

#     # --- Save CSV
#     file = f"{index_name.replace(' ', '_')}_Economic_Exposure.csv"
#     master.to_csv(file, index=False, encoding="utf-8-sig")
#     print(f"\nSaved → {file}")
    
#     return master

# # ==========================================================
# # RUN
# # ==========================================================
# if __name__ == "__main__":
#     df = Build_Economic_Map("NIFTY 500")
#     print(df.head(10))

























#---------------------------------------------------------------------------------------------------------------------------------

# import requests
# import json
# import pandas as pd
# from urllib.parse import quote

# # ==========================================================
# # Constants
# # ==========================================================
# BASE = "https://iislliveblob.niftyindices.com/jsonfiles"

# CATEGORIES = {
#     "MacroEconomicSector": "MacroEconomicSector",
#     "Sector": "Sector",
#     "Industry": "Industry",
#     "Basic Industry": "BasicIndustry"
# }

# # ==========================================================
# # Download & Normalize One NSE Category with WEIGHTS
# # ==========================================================
# def fetch_category(index_name, category):
#     folder = quote(category)
#     index  = quote(index_name)

#     url = f"{BASE}/{folder}/SectorialIndexData{index}_{category.replace(' ', '')}.js"
#     print(f"Fetching → {category}")

#     text = requests.get(url, timeout=15).text

#     # ---- Extract valid JSON from JS
#     start = text.find("{")
#     depth = 0
#     for i, ch in enumerate(text[start:], start=start):
#         if ch == "{": depth += 1
#         elif ch == "}":
#             depth -= 1
#             if depth == 0:
#                 end = i + 1
#                 break

#     data = json.loads(text[start:end].replace(",]", "]").replace(",}", "}"))

#     rows = []
#     for grp in data["groups"]:
#         group_name   = " ".join(grp["label"].split()[:-1])
#         group_weight = grp.get("weight", 0)

#         for stk in grp["groups"]:
#             stock = stk["label"].split()[0]
#             stock_weight = stk.get("weight", 0)

#             rows.append({
#                 "Stock": stock,
#                 CATEGORIES[category]: group_name,
#                 f"{CATEGORIES[category]}Weight%": group_weight,
#                 f"{CATEGORIES[category]}StockWeight%": stock_weight
#             })

#     return pd.DataFrame(rows)


# # ==========================================================
# # Build FULL Institutional Economic Exposure Map
# # ==========================================================
# def Build_Economic_Map(index_name):
#     # --- Fetch all category data
#     dfs = {cat: fetch_category(index_name, cat) for cat in CATEGORIES}

#     # --- Merge all on 'Stock'
#     master = dfs["MacroEconomicSector"] \
#         .merge(dfs["Sector"], on="Stock", how="left") \
#         .merge(dfs["Industry"], on="Stock", how="left") \
#         .merge(dfs["Basic Industry"], on="Stock", how="left")

#     # --- Keep stock-level weight (MacroEconomicSectorStockWeight%)
#     master["StockWeight%"] = master["MacroEconomicSectorStockWeight%"]

#     # --- Keep weights for hierarchical levels
#     master["MacroEconomicSectorWeight%"] = master["MacroEconomicSectorWeight%"]
#     master["SectorWeight%"] = master["SectorWeight%"]
#     master["IndustryWeight%"] = master["IndustryWeight%"]
#     master["BasicIndustryWeight%"] = master["BasicIndustryWeight%"]

#     # --- Final clean columns
#     master = master[[
#         "MacroEconomicSector", "MacroEconomicSectorWeight%",
#         "Sector", "SectorWeight%",
#         "Industry", "IndustryWeight%",
#         "BasicIndustry", "BasicIndustryWeight%",
#         "Stock", "StockWeight%"
#     ]]

#     # --- Sort nicely
#     master = master.sort_values(
#         ["MacroEconomicSector", "Sector", "Industry", "BasicIndustry", "Stock"]
#     )

#     # --- Save CSV
#     file = f"{index_name.replace(' ', '_')}_Economic_Exposure.csv"
#     master.to_csv(file, index=False, encoding="utf-8-sig")
#     print(f"\nSaved → {file}")

#     return master


# # ==========================================================
# # RUN
# # ==========================================================
# if __name__ == "__main__":
#     df = Build_Economic_Map("NIFTY 500")
#     print(df.head(10))
