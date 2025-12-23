# Coach Outreach Pro v5.0 Enterprise

A comprehensive college football recruiting outreach platform for high school athletes.

## 🚀 Quick Start

**Mac:** Double-click `start.command`
**Windows:** Double-click `START_WINDOWS.bat`

The app will:
1. Create a virtual environment (first run only)
2. Install dependencies automatically
3. Open your browser to http://localhost:5000

## ✨ New in v5.0 Enterprise

### 📧 Smart Email System
- **4 Pre-Built RC Templates** - Professional, varied styles
- **4 Pre-Built Position Coach Templates** - Tailored for OL/position coaches  
- **Template Randomization** - Each coach gets a different template automatically
- **Auto Follow-Up Scheduling** - Reminders at 7, 14, 21 days after sending

### 🔍 Google Twitter Scraper
- Find coach Twitter handles via Google search
- No Twitter API required
- Multi-query strategy for better results
- Handles validation and caching

### 📊 CRM System
- Pipeline tracking: Prospect → Contacted → Interested → Offer → Committed
- Contact management with notes
- Interaction history

### 📅 NCAA Calendar
- Dead/Quiet/Contact/Evaluation periods
- Key dates (Signing Days, Transfer Portal)
- Current period status check

### 🔔 Reminders
- Due date tracking
- Snooze and recurring options
- Dashboard view

### 📋 Reports
- Athlete one-pager PDFs
- Recruitment pipeline reports

## 📁 Project Structure

```
coach_outreach_v5/
├── app.py                    # Main Flask app (3700+ lines)
├── start.command             # Mac launcher
├── START_WINDOWS.bat         # Windows launcher
├── enterprise/               # Enterprise features
│   ├── templates.py          # 4 RC + 4 OC + 3 Follow-up templates
│   ├── followups.py          # Smart follow-up system
│   ├── twitter_google_scraper.py  # Google-based Twitter finder
│   ├── crm.py               # CRM system
│   ├── calendar.py          # NCAA calendar
│   ├── reminders.py         # Reminders system
│   └── schools_expanded.py  # +250 additional schools
├── outreach/
│   ├── email_sender.py      # Email with template randomization
│   └── twitter_sender.py    # Twitter DM
└── data/
    └── schools.py           # 161 base schools
```

## 🏈 School Database: 411 Total Schools
- FBS: 130 | FCS: 66 | D2: 107 | D3: 108

## 📧 Email Template Styles

**RC Templates:**
1. Formal Introduction - Professional, detailed
2. Energetic - Enthusiastic, shows passion
3. Brief & Direct - Gets to the point
4. Personal Story - Connects emotionally

**Position Coach Templates:**
1. Formal Position-Specific - Technical focus
2. Technique Focus - Shows coachability
3. Film-Forward Brief - Leads with film
4. Relationship Builder - Long-term focus

## ⚙️ Configuration

### Gmail Setup
1. Enable 2FA on your Gmail account
2. Generate App Password at https://myaccount.google.com/apppasswords
3. Enter email + app password in Connections

### Google Sheets
1. Create a Google Cloud project
2. Enable Sheets API
3. Create service account credentials
4. Share your spreadsheet with the service account

## 📝 Merge Fields

Templates support these variables:
- `{coach_name}` - Coach's last name
- `{school}` - School name
- `{athlete_name}` - Your full name
- `{position}` - Your position(s)
- `{grad_year}` - Graduation year
- `{height}`, `{weight}` - Physical stats
- `{gpa}` - Academic GPA
- `{hudl_link}` - Highlight film URL
- `{high_school}` - Your high school
- `{city_state}` - Your location
- `{phone}`, `{email}` - Contact info

---

**Coach Outreach Pro v5.0 Enterprise**
Built for athletes serious about their recruiting journey.
