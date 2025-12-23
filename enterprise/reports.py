"""
PDF Report Generator - Athlete one-pagers and recruitment reports
Generates HTML files that can be printed/saved as PDF
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import os


class ReportGenerator:
    """Generate athlete profile PDFs and recruitment reports"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_athlete_one_pager(self, athlete: Dict[str, Any], 
                                    output_path: Optional[str] = None) -> str:
        """Generate HTML athlete one-pager that can be printed to PDF"""
        name = athlete.get("name", "Athlete Name")
        grad_year = athlete.get("grad_year", "2025")
        position = athlete.get("position", "")
        secondary_position = athlete.get("secondary_position", "")
        high_school = athlete.get("high_school", "")
        city = athlete.get("city", "")
        state = athlete.get("state", "")
        height = athlete.get("height", "")
        weight = athlete.get("weight", "")
        forty = athlete.get("forty_time", "")
        shuttle = athlete.get("shuttle", "")
        vertical = athlete.get("vertical", "")
        broad_jump = athlete.get("broad_jump", "")
        bench = athlete.get("bench", "")
        squat = athlete.get("squat", "")
        gpa = athlete.get("gpa", "")
        sat = athlete.get("sat", "")
        act = athlete.get("act", "")
        email = athlete.get("email", "")
        phone = athlete.get("phone", "")
        twitter = athlete.get("twitter", "")
        hudl = athlete.get("hudl_link", "")
        
        pos_display = position
        if secondary_position:
            pos_display = f"{position} / {secondary_position}"
        
        hudl_html = ""
        if hudl:
            hudl_html = f'<div class="links"><a href="{hudl}" class="link-btn">View Hudl Highlights</a></div>'
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{name} - Athlete Profile</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: Arial, sans-serif; color: #333; background: #fff; }}
        .page {{ max-width: 8.5in; margin: 0 auto; padding: 0.5in; }}
        .header {{ display: flex; align-items: center; padding-bottom: 20px; border-bottom: 3px solid #1a365d; margin-bottom: 20px; }}
        .photo {{ width: 120px; height: 150px; background: #e2e8f0; border-radius: 8px; margin-right: 25px; display: flex; align-items: center; justify-content: center; color: #718096; font-size: 12px; }}
        .header-info {{ flex: 1; }}
        .name {{ font-size: 32px; font-weight: bold; color: #1a365d; margin-bottom: 5px; }}
        .position {{ font-size: 18px; color: #4a5568; margin-bottom: 5px; }}
        .school {{ font-size: 14px; color: #718096; }}
        .class-year {{ background: #1a365d; color: white; padding: 5px 15px; border-radius: 4px; font-size: 14px; font-weight: bold; display: inline-block; margin-top: 10px; }}
        .section {{ margin-bottom: 20px; }}
        .section-title {{ font-size: 14px; font-weight: bold; color: #1a365d; border-bottom: 2px solid #e2e8f0; padding-bottom: 5px; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
        .stat-box {{ background: #f7fafc; padding: 12px; border-radius: 6px; text-align: center; }}
        .stat-value {{ font-size: 20px; font-weight: bold; color: #1a365d; }}
        .stat-label {{ font-size: 11px; color: #718096; text-transform: uppercase; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
        .info-item {{ display: flex; }}
        .info-label {{ font-size: 12px; color: #718096; width: 80px; }}
        .info-value {{ font-size: 12px; font-weight: 500; }}
        .links {{ margin-top: 15px; }}
        .link-btn {{ background: #1a365d; color: white; padding: 8px 16px; border-radius: 4px; text-decoration: none; font-size: 12px; font-weight: 500; }}
        .footer {{ margin-top: 20px; padding-top: 15px; border-top: 1px solid #e2e8f0; text-align: center; font-size: 11px; color: #a0aec0; }}
        @media print {{ .page {{ padding: 0.25in; }} }}
    </style>
</head>
<body>
    <div class="page">
        <div class="header">
            <div class="photo">Photo</div>
            <div class="header-info">
                <div class="name">{name}</div>
                <div class="position">{pos_display}</div>
                <div class="school">{high_school} - {city}, {state}</div>
                <div class="class-year">Class of {grad_year}</div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-title">Physical Measurements</div>
            <div class="stats-grid">
                <div class="stat-box"><div class="stat-value">{height or '-'}</div><div class="stat-label">Height</div></div>
                <div class="stat-box"><div class="stat-value">{weight or '-'}</div><div class="stat-label">Weight</div></div>
                <div class="stat-box"><div class="stat-value">{forty or '-'}</div><div class="stat-label">40-Yard</div></div>
                <div class="stat-box"><div class="stat-value">{shuttle or '-'}</div><div class="stat-label">Shuttle</div></div>
                <div class="stat-box"><div class="stat-value">{vertical or '-'}</div><div class="stat-label">Vertical</div></div>
                <div class="stat-box"><div class="stat-value">{broad_jump or '-'}</div><div class="stat-label">Broad Jump</div></div>
                <div class="stat-box"><div class="stat-value">{bench or '-'}</div><div class="stat-label">Bench</div></div>
                <div class="stat-box"><div class="stat-value">{squat or '-'}</div><div class="stat-label">Squat</div></div>
            </div>
        </div>
        
        <div class="two-col">
            <div class="section">
                <div class="section-title">Academics</div>
                <div class="info-grid">
                    <div class="info-item"><span class="info-label">GPA:</span><span class="info-value">{gpa or '-'}</span></div>
                    <div class="info-item"><span class="info-label">SAT:</span><span class="info-value">{sat or '-'}</span></div>
                    <div class="info-item"><span class="info-label">ACT:</span><span class="info-value">{act or '-'}</span></div>
                </div>
            </div>
            <div class="section">
                <div class="section-title">Contact Information</div>
                <div class="info-grid">
                    <div class="info-item"><span class="info-label">Email:</span><span class="info-value">{email or '-'}</span></div>
                    <div class="info-item"><span class="info-label">Phone:</span><span class="info-value">{phone or '-'}</span></div>
                    <div class="info-item"><span class="info-label">Twitter:</span><span class="info-value">{twitter or '-'}</span></div>
                </div>
            </div>
        </div>
        
        {hudl_html}
        
        <div class="footer">Generated by Coach Outreach Pro - {datetime.now().strftime("%B %d, %Y")}</div>
    </div>
</body>
</html>"""
        
        if output_path is None:
            safe_name = name.replace(" ", "_").lower()
            output_path = os.path.join(self.output_dir, f"{safe_name}_profile.html")
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        return output_path
    
    def generate_recruitment_report(self, contacts: List[Dict[str, Any]], 
                                     athlete_name: str = "Athlete",
                                     output_path: Optional[str] = None) -> str:
        """Generate recruitment status report"""
        
        stages = {}
        for contact in contacts:
            stage = contact.get("stage", "prospect")
            if stage not in stages:
                stages[stage] = []
            stages[stage].append(contact)
        
        stage_colors = {
            "prospect": "#6b7280", "contacted": "#3b82f6", "interested": "#8b5cf6",
            "evaluating": "#f59e0b", "verbal_offer": "#10b981", "committed": "#22c55e",
            "signed": "#059669", "declined": "#ef4444"
        }
        
        rows_html = ""
        stage_order = ["signed", "committed", "verbal_offer", "evaluating", "interested", "contacted", "prospect", "declined"]
        for stage in stage_order:
            if stage in stages:
                for c in stages[stage]:
                    color = stage_colors.get(stage, "#6b7280")
                    last = c.get("last_contact", "-")
                    rows_html += f'<tr><td>{c.get("school_name", "")}</td><td>{c.get("coach_name", "")}</td><td>{c.get("title", "")}</td><td><span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:11px;">{stage.replace("_", " ").title()}</span></td><td>{last}</td></tr>'
        
        offers = len(stages.get("verbal_offer", [])) + len(stages.get("committed", [])) + len(stages.get("signed", []))
        active = len(stages.get("interested", [])) + len(stages.get("evaluating", []))
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Recruitment Report - {athlete_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; padding: 40px; }}
        h1 {{ color: #1a365d; margin-bottom: 5px; }}
        .subtitle {{ color: #718096; margin-bottom: 30px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background: #1a365d; color: white; padding: 12px; text-align: left; font-size: 12px; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #e2e8f0; font-size: 13px; }}
        .summary {{ display: flex; gap: 20px; margin-bottom: 30px; }}
        .summary-box {{ background: #f7fafc; padding: 20px; border-radius: 8px; flex: 1; text-align: center; }}
        .summary-value {{ font-size: 28px; font-weight: bold; color: #1a365d; }}
        .summary-label {{ font-size: 12px; color: #718096; }}
    </style>
</head>
<body>
    <h1>Recruitment Report</h1>
    <div class="subtitle">{athlete_name} - Generated {datetime.now().strftime("%B %d, %Y")}</div>
    <div class="summary">
        <div class="summary-box"><div class="summary-value">{len(contacts)}</div><div class="summary-label">Total Schools</div></div>
        <div class="summary-box"><div class="summary-value">{offers}</div><div class="summary-label">Offers</div></div>
        <div class="summary-box"><div class="summary-value">{active}</div><div class="summary-label">Active Interest</div></div>
    </div>
    <table><thead><tr><th>School</th><th>Coach</th><th>Title</th><th>Status</th><th>Last Contact</th></tr></thead><tbody>{rows_html}</tbody></table>
</body>
</html>"""
        
        if output_path is None:
            safe_name = athlete_name.replace(" ", "_").lower()
            output_path = os.path.join(self.output_dir, f"{safe_name}_recruitment.html")
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        return output_path
