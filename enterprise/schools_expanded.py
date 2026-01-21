"""
Expanded Schools Database - Additional 500+ College Football Programs
Supplements the base schools.py with FCS, D2, and D3 programs
"""

from typing import List, Dict, Any

# Additional schools organized by conference
EXPANDED_SCHOOLS = [
    # =========== FCS SCHOOLS ===========
    # Southland Conference (8)
    {"name": "Incarnate Word", "state": "TX", "city": "San Antonio", "conference": "Southland", "division": "FCS", "public": False, "enrollment": 8000},
    {"name": "Lamar", "state": "TX", "city": "Beaumont", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 16000},
    {"name": "McNeese", "state": "LA", "city": "Lake Charles", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 8000},
    {"name": "Nicholls", "state": "LA", "city": "Thibodaux", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 6000},
    {"name": "Northwestern State", "state": "LA", "city": "Natchitoches", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 11000},
    {"name": "SE Louisiana", "state": "LA", "city": "Hammond", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 14000},
    {"name": "Houston Christian", "state": "TX", "city": "Houston", "conference": "Southland", "division": "FCS", "public": False, "enrollment": 4000},
    {"name": "Texas A&M-Commerce", "state": "TX", "city": "Commerce", "conference": "Southland", "division": "FCS", "public": True, "enrollment": 12000},
    # SWAC (12)
    {"name": "Jackson State", "state": "MS", "city": "Jackson", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 7000},
    {"name": "Florida A&M", "state": "FL", "city": "Tallahassee", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 10000},
    {"name": "Southern", "state": "LA", "city": "Baton Rouge", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 7000},
    {"name": "Grambling State", "state": "LA", "city": "Grambling", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 5000},
    {"name": "Alabama A&M", "state": "AL", "city": "Huntsville", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 6000},
    {"name": "Alabama State", "state": "AL", "city": "Montgomery", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 4000},
    {"name": "Prairie View A&M", "state": "TX", "city": "Prairie View", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 9000},
    {"name": "Texas Southern", "state": "TX", "city": "Houston", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 9000},
    {"name": "Alcorn State", "state": "MS", "city": "Lorman", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 3000},
    {"name": "Bethune-Cookman", "state": "FL", "city": "Daytona Beach", "conference": "SWAC", "division": "FCS", "public": False, "enrollment": 3000},
    {"name": "Arkansas-Pine Bluff", "state": "AR", "city": "Pine Bluff", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 2500},
    {"name": "Mississippi Valley State", "state": "MS", "city": "Itta Bena", "conference": "SWAC", "division": "FCS", "public": True, "enrollment": 2000},
    # MEAC (6)
    {"name": "North Carolina A&T", "state": "NC", "city": "Greensboro", "conference": "MEAC", "division": "FCS", "public": True, "enrollment": 13000},
    {"name": "Howard", "state": "DC", "city": "Washington", "conference": "MEAC", "division": "FCS", "public": False, "enrollment": 10000},
    {"name": "Morgan State", "state": "MD", "city": "Baltimore", "conference": "MEAC", "division": "FCS", "public": True, "enrollment": 8000},
    {"name": "Norfolk State", "state": "VA", "city": "Norfolk", "conference": "MEAC", "division": "FCS", "public": True, "enrollment": 6000},
    {"name": "SC State", "state": "SC", "city": "Orangeburg", "conference": "MEAC", "division": "FCS", "public": True, "enrollment": 3000},
    {"name": "Delaware State", "state": "DE", "city": "Dover", "conference": "MEAC", "division": "FCS", "public": True, "enrollment": 5000},
    # NEC (8)
    {"name": "Duquesne", "state": "PA", "city": "Pittsburgh", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 9000},
    {"name": "Sacred Heart", "state": "CT", "city": "Fairfield", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 9000},
    {"name": "Central Connecticut", "state": "CT", "city": "New Britain", "conference": "NEC", "division": "FCS", "public": True, "enrollment": 11000},
    {"name": "LIU", "state": "NY", "city": "Brooklyn", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 15000},
    {"name": "Wagner", "state": "NY", "city": "Staten Island", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 2000},
    {"name": "St. Francis (PA)", "state": "PA", "city": "Loretto", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 2500},
    {"name": "Merrimack", "state": "MA", "city": "North Andover", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 5000},
    {"name": "Stonehill", "state": "MA", "city": "Easton", "conference": "NEC", "division": "FCS", "public": False, "enrollment": 3000},
    # OVC (7)
    {"name": "Eastern Illinois", "state": "IL", "city": "Charleston", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 8000},
    {"name": "Southeast Missouri", "state": "MO", "city": "Cape Girardeau", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 11000},
    {"name": "Tennessee Tech", "state": "TN", "city": "Cookeville", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 10000},
    {"name": "Tennessee State", "state": "TN", "city": "Nashville", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 8000},
    {"name": "UT Martin", "state": "TN", "city": "Martin", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 7000},
    {"name": "Lindenwood", "state": "MO", "city": "St. Charles", "conference": "OVC", "division": "FCS", "public": False, "enrollment": 7000},
    {"name": "Southern Indiana", "state": "IN", "city": "Evansville", "conference": "OVC", "division": "FCS", "public": True, "enrollment": 9000},
    # Pioneer (10)
    {"name": "Dayton", "state": "OH", "city": "Dayton", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 12000},
    {"name": "Drake", "state": "IA", "city": "Des Moines", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 5000},
    {"name": "Butler", "state": "IN", "city": "Indianapolis", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 5500},
    {"name": "Valparaiso", "state": "IN", "city": "Valparaiso", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 4000},
    {"name": "San Diego", "state": "CA", "city": "San Diego", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 9000},
    {"name": "Stetson", "state": "FL", "city": "DeLand", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 4500},
    {"name": "Davidson", "state": "NC", "city": "Davidson", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 2000},
    {"name": "Marist", "state": "NY", "city": "Poughkeepsie", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 7000},
    {"name": "Presbyterian", "state": "SC", "city": "Clinton", "conference": "Pioneer", "division": "FCS", "public": False, "enrollment": 1400},
    {"name": "Morehead State", "state": "KY", "city": "Morehead", "conference": "Pioneer", "division": "FCS", "public": True, "enrollment": 10000},
    
    # =========== D2 SCHOOLS ===========
    # GLIAC (13)
    {"name": "Ferris State", "state": "MI", "city": "Big Rapids", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 14000},
    {"name": "Grand Valley State", "state": "MI", "city": "Allendale", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 24000},
    {"name": "Saginaw Valley State", "state": "MI", "city": "University Center", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 8500},
    {"name": "Michigan Tech", "state": "MI", "city": "Houghton", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Northern Michigan", "state": "MI", "city": "Marquette", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 7500},
    {"name": "Wayne State (MI)", "state": "MI", "city": "Detroit", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": 27000},
    {"name": "Ashland", "state": "OH", "city": "Ashland", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 6000},
    {"name": "Findlay", "state": "OH", "city": "Findlay", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 4000},
    {"name": "Hillsdale", "state": "MI", "city": "Hillsdale", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 1500},
    {"name": "Northwood", "state": "MI", "city": "Midland", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 3000},
    {"name": "Davenport", "state": "MI", "city": "Grand Rapids", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 7000},
    {"name": "Tiffin", "state": "OH", "city": "Tiffin", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 3000},
    {"name": "Walsh", "state": "OH", "city": "North Canton", "conference": "GLIAC", "division": "D2", "public": False, "enrollment": 2500},
    # MIAA (12)
    {"name": "Northwest Missouri State", "state": "MO", "city": "Maryville", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Pittsburg State", "state": "KS", "city": "Pittsburg", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Central Missouri", "state": "MO", "city": "Warrensburg", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 11000},
    {"name": "Fort Hays State", "state": "KS", "city": "Hays", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 16000},
    {"name": "Emporia State", "state": "KS", "city": "Emporia", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Missouri Western", "state": "MO", "city": "St. Joseph", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 5000},
    {"name": "Missouri Southern", "state": "MO", "city": "Joplin", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Nebraska-Kearney", "state": "NE", "city": "Kearney", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Washburn", "state": "KS", "city": "Topeka", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Central Oklahoma", "state": "OK", "city": "Edmond", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 17000},
    {"name": "Northeastern State", "state": "OK", "city": "Tahlequah", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 8000},
    {"name": "Lincoln (MO)", "state": "MO", "city": "Jefferson City", "conference": "MIAA", "division": "D2", "public": True, "enrollment": 3000},
    # Gulf South (8)
    {"name": "Valdosta State", "state": "GA", "city": "Valdosta", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": 11000},
    {"name": "West Georgia", "state": "GA", "city": "Carrollton", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": 13000},
    {"name": "West Florida", "state": "FL", "city": "Pensacola", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": 13000},
    {"name": "Delta State", "state": "MS", "city": "Cleveland", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "West Alabama", "state": "AL", "city": "Livingston", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Mississippi College", "state": "MS", "city": "Clinton", "conference": "Gulf South", "division": "D2", "public": False, "enrollment": 5000},
    {"name": "Shorter", "state": "GA", "city": "Rome", "conference": "Gulf South", "division": "D2", "public": False, "enrollment": 2500},
    {"name": "North Greenville", "state": "SC", "city": "Tigerville", "conference": "Gulf South", "division": "D2", "public": False, "enrollment": 2500},
    # PSAC (12)
    {"name": "West Chester", "state": "PA", "city": "West Chester", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 18000},
    {"name": "Slippery Rock", "state": "PA", "city": "Slippery Rock", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 9000},
    {"name": "Kutztown", "state": "PA", "city": "Kutztown", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 9000},
    {"name": "Indiana (PA)", "state": "PA", "city": "Indiana", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 11000},
    {"name": "California (PA)", "state": "PA", "city": "California", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Bloomsburg", "state": "PA", "city": "Bloomsburg", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 9000},
    {"name": "Shippensburg", "state": "PA", "city": "Shippensburg", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Millersville", "state": "PA", "city": "Millersville", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 8000},
    {"name": "East Stroudsburg", "state": "PA", "city": "East Stroudsburg", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Lock Haven", "state": "PA", "city": "Lock Haven", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Shepherd", "state": "WV", "city": "Shepherdstown", "conference": "PSAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Gannon", "state": "PA", "city": "Erie", "conference": "PSAC", "division": "D2", "public": False, "enrollment": 4000},
    # NSIC (14)
    {"name": "Minnesota State Mankato", "state": "MN", "city": "Mankato", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 15000},
    {"name": "Augustana (SD)", "state": "SD", "city": "Sioux Falls", "conference": "NSIC", "division": "D2", "public": False, "enrollment": 2000},
    {"name": "St. Cloud State", "state": "MN", "city": "St. Cloud", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 16000},
    {"name": "Winona State", "state": "MN", "city": "Winona", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 8000},
    {"name": "Minnesota Duluth", "state": "MN", "city": "Duluth", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 10000},
    {"name": "Bemidji State", "state": "MN", "city": "Bemidji", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 5000},
    {"name": "Southwest Minnesota State", "state": "MN", "city": "Marshall", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 7000},
    {"name": "Wayne State (NE)", "state": "NE", "city": "Wayne", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Upper Iowa", "state": "IA", "city": "Fayette", "conference": "NSIC", "division": "D2", "public": False, "enrollment": 5000},
    {"name": "Sioux Falls", "state": "SD", "city": "Sioux Falls", "conference": "NSIC", "division": "D2", "public": False, "enrollment": 1500},
    {"name": "Northern State", "state": "SD", "city": "Aberdeen", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 3500},
    {"name": "Mary", "state": "ND", "city": "Bismarck", "conference": "NSIC", "division": "D2", "public": False, "enrollment": 4000},
    {"name": "Minot State", "state": "ND", "city": "Minot", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "MSU Moorhead", "state": "MN", "city": "Moorhead", "conference": "NSIC", "division": "D2", "public": True, "enrollment": 6000},
    # RMAC (10)
    {"name": "Colorado School of Mines", "state": "CO", "city": "Golden", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 6500},
    {"name": "CSU Pueblo", "state": "CO", "city": "Pueblo", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Chadron State", "state": "NE", "city": "Chadron", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "Black Hills State", "state": "SD", "city": "Spearfish", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "South Dakota Mines", "state": "SD", "city": "Rapid City", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "Adams State", "state": "CO", "city": "Alamosa", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "Western Colorado", "state": "CO", "city": "Gunnison", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3000},
    {"name": "Fort Lewis", "state": "CO", "city": "Durango", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3500},
    {"name": "New Mexico Highlands", "state": "NM", "city": "Las Vegas", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3500},
    {"name": "Western New Mexico", "state": "NM", "city": "Silver City", "conference": "RMAC", "division": "D2", "public": True, "enrollment": 3000},
    # SAC (10)
    {"name": "Lenoir-Rhyne", "state": "NC", "city": "Hickory", "conference": "SAC", "division": "D2", "public": False, "enrollment": 2500},
    {"name": "Wingate", "state": "NC", "city": "Wingate", "conference": "SAC", "division": "D2", "public": False, "enrollment": 3600},
    {"name": "Carson-Newman", "state": "TN", "city": "Jefferson City", "conference": "SAC", "division": "D2", "public": False, "enrollment": 2500},
    {"name": "Tusculum", "state": "TN", "city": "Greeneville", "conference": "SAC", "division": "D2", "public": False, "enrollment": 2500},
    {"name": "Catawba", "state": "NC", "city": "Salisbury", "conference": "SAC", "division": "D2", "public": False, "enrollment": 1300},
    {"name": "Mars Hill", "state": "NC", "city": "Mars Hill", "conference": "SAC", "division": "D2", "public": False, "enrollment": 1500},
    {"name": "Newberry", "state": "SC", "city": "Newberry", "conference": "SAC", "division": "D2", "public": False, "enrollment": 1200},
    {"name": "Limestone", "state": "SC", "city": "Gaffney", "conference": "SAC", "division": "D2", "public": False, "enrollment": 3500},
    {"name": "Anderson (SC)", "state": "SC", "city": "Anderson", "conference": "SAC", "division": "D2", "public": False, "enrollment": 4000},
    {"name": "Barton", "state": "NC", "city": "Wilson", "conference": "SAC", "division": "D2", "public": False, "enrollment": 1200},
    # LSC (10)
    {"name": "Angelo State", "state": "TX", "city": "San Angelo", "conference": "LSC", "division": "D2", "public": True, "enrollment": 10000},
    {"name": "West Texas A&M", "state": "TX", "city": "Canyon", "conference": "LSC", "division": "D2", "public": True, "enrollment": 10000},
    {"name": "Texas A&M-Kingsville", "state": "TX", "city": "Kingsville", "conference": "LSC", "division": "D2", "public": True, "enrollment": 8000},
    {"name": "Tarleton State", "state": "TX", "city": "Stephenville", "conference": "LSC", "division": "D2", "public": True, "enrollment": 14000},
    {"name": "Midwestern State", "state": "TX", "city": "Wichita Falls", "conference": "LSC", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Eastern New Mexico", "state": "NM", "city": "Portales", "conference": "LSC", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "UT Permian Basin", "state": "TX", "city": "Odessa", "conference": "LSC", "division": "D2", "public": True, "enrollment": 6000},
    {"name": "Western Oregon", "state": "OR", "city": "Monmouth", "conference": "LSC", "division": "D2", "public": True, "enrollment": 5000},
    {"name": "Azusa Pacific", "state": "CA", "city": "Azusa", "conference": "LSC", "division": "D2", "public": False, "enrollment": 10000},
    {"name": "Colorado Mesa", "state": "CO", "city": "Grand Junction", "conference": "LSC", "division": "D2", "public": True, "enrollment": 11000},
    # GAC (10)
    {"name": "Harding", "state": "AR", "city": "Searcy", "conference": "GAC", "division": "D2", "public": False, "enrollment": 5000},
    {"name": "Ouachita Baptist", "state": "AR", "city": "Arkadelphia", "conference": "GAC", "division": "D2", "public": False, "enrollment": 1600},
    {"name": "Henderson State", "state": "AR", "city": "Arkadelphia", "conference": "GAC", "division": "D2", "public": True, "enrollment": 3500},
    {"name": "Arkansas Tech", "state": "AR", "city": "Russellville", "conference": "GAC", "division": "D2", "public": True, "enrollment": 10000},
    {"name": "Southern Arkansas", "state": "AR", "city": "Magnolia", "conference": "GAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Oklahoma Baptist", "state": "OK", "city": "Shawnee", "conference": "GAC", "division": "D2", "public": False, "enrollment": 2100},
    {"name": "East Central", "state": "OK", "city": "Ada", "conference": "GAC", "division": "D2", "public": True, "enrollment": 4000},
    {"name": "Southeastern Oklahoma", "state": "OK", "city": "Durant", "conference": "GAC", "division": "D2", "public": True, "enrollment": 5000},
    {"name": "Northwestern Oklahoma", "state": "OK", "city": "Alva", "conference": "GAC", "division": "D2", "public": True, "enrollment": 2000},
    {"name": "Southwestern Oklahoma", "state": "OK", "city": "Weatherford", "conference": "GAC", "division": "D2", "public": True, "enrollment": 5000},
    
    # =========== D3 SCHOOLS ===========
    # WIAC (8)
    {"name": "Wisconsin-Whitewater", "state": "WI", "city": "Whitewater", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 12000},
    {"name": "UW-Oshkosh", "state": "WI", "city": "Oshkosh", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 14000},
    {"name": "UW-Platteville", "state": "WI", "city": "Platteville", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 8000},
    {"name": "UW-La Crosse", "state": "WI", "city": "La Crosse", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 10000},
    {"name": "UW-Stevens Point", "state": "WI", "city": "Stevens Point", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 9000},
    {"name": "UW-Stout", "state": "WI", "city": "Menomonie", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 9000},
    {"name": "UW-Eau Claire", "state": "WI", "city": "Eau Claire", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 11000},
    {"name": "UW-River Falls", "state": "WI", "city": "River Falls", "conference": "WIAC", "division": "D3", "public": True, "enrollment": 6000},
    # OAC (10)
    {"name": "Mount Union", "state": "OH", "city": "Alliance", "conference": "OAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "John Carroll", "state": "OH", "city": "University Heights", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Baldwin Wallace", "state": "OH", "city": "Berea", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3500},
    {"name": "Ohio Northern", "state": "OH", "city": "Ada", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Capital", "state": "OH", "city": "Columbus", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Otterbein", "state": "OH", "city": "Westerville", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Heidelberg", "state": "OH", "city": "Tiffin", "conference": "OAC", "division": "D3", "public": False, "enrollment": 1200},
    {"name": "Marietta", "state": "OH", "city": "Marietta", "conference": "OAC", "division": "D3", "public": False, "enrollment": 1400},
    {"name": "Muskingum", "state": "OH", "city": "New Concord", "conference": "OAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Wilmington", "state": "OH", "city": "Wilmington", "conference": "OAC", "division": "D3", "public": False, "enrollment": 1500},
    # CCIW (9)
    {"name": "North Central (IL)", "state": "IL", "city": "Naperville", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Wheaton (IL)", "state": "IL", "city": "Wheaton", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Augustana (IL)", "state": "IL", "city": "Rock Island", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 2500},
    {"name": "Illinois Wesleyan", "state": "IL", "city": "Bloomington", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 1800},
    {"name": "Millikin", "state": "IL", "city": "Decatur", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Carthage", "state": "WI", "city": "Kenosha", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Elmhurst", "state": "IL", "city": "Elmhurst", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 3500},
    {"name": "North Park", "state": "IL", "city": "Chicago", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Washington (MO)", "state": "MO", "city": "St. Louis", "conference": "CCIW", "division": "D3", "public": False, "enrollment": 15000},
    # MIAC (9)
    {"name": "St. Johns (MN)", "state": "MN", "city": "Collegeville", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 1800},
    {"name": "Bethel (MN)", "state": "MN", "city": "St. Paul", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 6000},
    {"name": "Concordia (MN)", "state": "MN", "city": "Moorhead", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Gustavus Adolphus", "state": "MN", "city": "St. Peter", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 2400},
    {"name": "Augsburg", "state": "MN", "city": "Minneapolis", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 3500},
    {"name": "Hamline", "state": "MN", "city": "St. Paul", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 5000},
    {"name": "St. Olaf", "state": "MN", "city": "Northfield", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Carleton", "state": "MN", "city": "Northfield", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Macalester", "state": "MN", "city": "St. Paul", "conference": "MIAC", "division": "D3", "public": False, "enrollment": 2200},
    # NESCAC (10)
    {"name": "Williams", "state": "MA", "city": "Williamstown", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 2100},
    {"name": "Amherst", "state": "MA", "city": "Amherst", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 1900},
    {"name": "Middlebury", "state": "VT", "city": "Middlebury", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 2600},
    {"name": "Wesleyan", "state": "CT", "city": "Middletown", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 3200},
    {"name": "Trinity (CT)", "state": "CT", "city": "Hartford", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 2200},
    {"name": "Bowdoin", "state": "ME", "city": "Brunswick", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 1900},
    {"name": "Colby", "state": "ME", "city": "Waterville", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Bates", "state": "ME", "city": "Lewiston", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 1800},
    {"name": "Hamilton", "state": "NY", "city": "Clinton", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Tufts", "state": "MA", "city": "Medford", "conference": "NESCAC", "division": "D3", "public": False, "enrollment": 12000},
    # Centennial (10)
    {"name": "Johns Hopkins", "state": "MD", "city": "Baltimore", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 27000},
    {"name": "Franklin & Marshall", "state": "PA", "city": "Lancaster", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 2300},
    {"name": "Gettysburg", "state": "PA", "city": "Gettysburg", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 2600},
    {"name": "Dickinson", "state": "PA", "city": "Carlisle", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 2400},
    {"name": "Muhlenberg", "state": "PA", "city": "Allentown", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 2400},
    {"name": "Ursinus", "state": "PA", "city": "Collegeville", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 1400},
    {"name": "McDaniel", "state": "MD", "city": "Westminster", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 1700},
    {"name": "Susquehanna", "state": "PA", "city": "Selinsgrove", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 2200},
    {"name": "Juniata", "state": "PA", "city": "Huntingdon", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 1400},
    {"name": "Moravian", "state": "PA", "city": "Bethlehem", "conference": "Centennial", "division": "D3", "public": False, "enrollment": 3200},
    # UAA (8)
    {"name": "Carnegie Mellon", "state": "PA", "city": "Pittsburgh", "conference": "UAA", "division": "D3", "public": False, "enrollment": 15000},
    {"name": "Case Western Reserve", "state": "OH", "city": "Cleveland", "conference": "UAA", "division": "D3", "public": False, "enrollment": 12000},
    {"name": "Chicago", "state": "IL", "city": "Chicago", "conference": "UAA", "division": "D3", "public": False, "enrollment": 18000},
    {"name": "Emory", "state": "GA", "city": "Atlanta", "conference": "UAA", "division": "D3", "public": False, "enrollment": 15000},
    {"name": "Rochester", "state": "NY", "city": "Rochester", "conference": "UAA", "division": "D3", "public": False, "enrollment": 12000},
    {"name": "Wash U St. Louis", "state": "MO", "city": "St. Louis", "conference": "UAA", "division": "D3", "public": False, "enrollment": 16000},
    {"name": "Brandeis", "state": "MA", "city": "Waltham", "conference": "UAA", "division": "D3", "public": False, "enrollment": 6000},
    {"name": "NYU", "state": "NY", "city": "New York", "conference": "UAA", "division": "D3", "public": False, "enrollment": 52000},
    # ASC (10)
    {"name": "Mary Hardin-Baylor", "state": "TX", "city": "Belton", "conference": "ASC", "division": "D3", "public": False, "enrollment": 4000},
    {"name": "Hardin-Simmons", "state": "TX", "city": "Abilene", "conference": "ASC", "division": "D3", "public": False, "enrollment": 2200},
    {"name": "East Texas Baptist", "state": "TX", "city": "Marshall", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1600},
    {"name": "Texas Lutheran", "state": "TX", "city": "Seguin", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1400},
    {"name": "Southwestern (TX)", "state": "TX", "city": "Georgetown", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1500},
    {"name": "Howard Payne", "state": "TX", "city": "Brownwood", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1000},
    {"name": "McMurry", "state": "TX", "city": "Abilene", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1100},
    {"name": "Sul Ross State", "state": "TX", "city": "Alpine", "conference": "ASC", "division": "D3", "public": True, "enrollment": 2000},
    {"name": "Austin College", "state": "TX", "city": "Sherman", "conference": "ASC", "division": "D3", "public": False, "enrollment": 1300},
    {"name": "Trinity (TX)", "state": "TX", "city": "San Antonio", "conference": "ASC", "division": "D3", "public": False, "enrollment": 2600},
    # NJAC (8)
    {"name": "Rowan", "state": "NJ", "city": "Glassboro", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 20000},
    {"name": "TCNJ", "state": "NJ", "city": "Ewing", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 7500},
    {"name": "Montclair State", "state": "NJ", "city": "Montclair", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 21000},
    {"name": "William Paterson", "state": "NJ", "city": "Wayne", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 11000},
    {"name": "Kean", "state": "NJ", "city": "Union", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 16000},
    {"name": "Ramapo", "state": "NJ", "city": "Mahwah", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 6000},
    {"name": "Wesley", "state": "DE", "city": "Dover", "conference": "NJAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Christopher Newport", "state": "VA", "city": "Newport News", "conference": "NJAC", "division": "D3", "public": True, "enrollment": 5000},
    # NWC (8)
    {"name": "Linfield", "state": "OR", "city": "McMinnville", "conference": "NWC", "division": "D3", "public": False, "enrollment": 2200},
    {"name": "Pacific Lutheran", "state": "WA", "city": "Tacoma", "conference": "NWC", "division": "D3", "public": False, "enrollment": 3200},
    {"name": "Puget Sound", "state": "WA", "city": "Tacoma", "conference": "NWC", "division": "D3", "public": False, "enrollment": 2600},
    {"name": "Willamette", "state": "OR", "city": "Salem", "conference": "NWC", "division": "D3", "public": False, "enrollment": 2900},
    {"name": "Whitworth", "state": "WA", "city": "Spokane", "conference": "NWC", "division": "D3", "public": False, "enrollment": 3000},
    {"name": "Lewis & Clark", "state": "OR", "city": "Portland", "conference": "NWC", "division": "D3", "public": False, "enrollment": 3500},
    {"name": "George Fox", "state": "OR", "city": "Newberg", "conference": "NWC", "division": "D3", "public": False, "enrollment": 4000},
    {"name": "Pacific (OR)", "state": "OR", "city": "Forest Grove", "conference": "NWC", "division": "D3", "public": False, "enrollment": 3700},
    # NCAC (10)
    {"name": "Wittenberg", "state": "OH", "city": "Springfield", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 1700},
    {"name": "Denison", "state": "OH", "city": "Granville", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 2400},
    {"name": "Wooster", "state": "OH", "city": "Wooster", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 2000},
    {"name": "Kenyon", "state": "OH", "city": "Gambier", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 1700},
    {"name": "Oberlin", "state": "OH", "city": "Oberlin", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 2900},
    {"name": "Hiram", "state": "OH", "city": "Hiram", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 1200},
    {"name": "Allegheny", "state": "PA", "city": "Meadville", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 1800},
    {"name": "DePauw", "state": "IN", "city": "Greencastle", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 2300},
    {"name": "Wabash", "state": "IN", "city": "Crawfordsville", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 850},
    {"name": "Ohio Wesleyan", "state": "OH", "city": "Delaware", "conference": "NCAC", "division": "D3", "public": False, "enrollment": 1600},
]

def get_all_schools() -> List[Dict[str, Any]]:
    """Get combined list of base schools + expanded schools"""
    try:
        from data.schools import SCHOOLS as BASE_SCHOOLS
        # Filter out any duplicates
        base_names = {s["name"] for s in BASE_SCHOOLS}
        additional = [s for s in EXPANDED_SCHOOLS if s["name"] not in base_names]
        return BASE_SCHOOLS + additional
    except ImportError:
        return EXPANDED_SCHOOLS

def get_school_count() -> Dict[str, int]:
    """Get count of schools by division"""
    all_schools = get_all_schools()
    counts = {"FBS": 0, "FCS": 0, "D2": 0, "D3": 0, "Total": 0}
    for school in all_schools:
        div = school.get("division", "Unknown")
        if div in counts:
            counts[div] += 1
        counts["Total"] += 1
    return counts
