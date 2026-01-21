"""
Enterprise API Routes - CRM, Reminders, Reports
Add these routes to the main Flask app
"""

from flask import Blueprint, jsonify, request
from datetime import datetime
import uuid
import os

# Import enterprise modules
from enterprise.crm import CRMManager, Contact, Interaction, PipelineStage, InteractionType
from enterprise.reminders import ReminderManager, Reminder, ReminderType, ReminderPriority
from enterprise.schools_expanded import EXPANDED_SCHOOLS, get_all_schools
from enterprise.reports import ReportGenerator

# Create blueprint
enterprise_bp = Blueprint('enterprise', __name__)

# Initialize managers (will use app's data directory)
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
crm = CRMManager(data_dir=DATA_DIR)
reminders = ReminderManager(data_dir=DATA_DIR)
reports = ReportGenerator(output_dir=os.path.join(DATA_DIR, 'reports'))

# ============================================================================
# CRM ROUTES
# ============================================================================

@enterprise_bp.route('/api/crm/contacts', methods=['GET'])
def get_crm_contacts():
    """Get all CRM contacts"""
    stage = request.args.get('stage')
    school = request.args.get('school')
    search = request.args.get('search')
    
    if search:
        contacts = crm.search_contacts(search)
    elif stage:
        try:
            contacts = crm.get_contacts_by_stage(PipelineStage(stage))
        except ValueError:
            contacts = crm.get_all_contacts()
    elif school:
        contacts = crm.get_contacts_by_school(school)
    else:
        contacts = crm.get_all_contacts()
    
    return jsonify({
        'success': True,
        'contacts': [c.to_dict() for c in contacts],
        'count': len(contacts)
    })

@enterprise_bp.route('/api/crm/contacts', methods=['POST'])
def add_crm_contact():
    """Add a new CRM contact"""
    data = request.json
    
    contact = Contact(
        id=data.get('id', str(uuid.uuid4())),
        school_name=data.get('school_name', ''),
        coach_name=data.get('coach_name', ''),
        title=data.get('title', ''),
        email=data.get('email', ''),
        phone=data.get('phone', ''),
        twitter=data.get('twitter', ''),
        stage=PipelineStage(data.get('stage', 'prospect')),
        notes=data.get('notes', ''),
        tags=data.get('tags', []),
        priority=data.get('priority', 1),
        interest_level=data.get('interest_level', 0)
    )
    
    result = crm.add_contact(contact)
    return jsonify({'success': True, 'contact': result.to_dict()})

@enterprise_bp.route('/api/crm/contacts/<contact_id>', methods=['PUT'])
def update_crm_contact(contact_id):
    """Update a CRM contact"""
    data = request.json
    result = crm.update_contact(contact_id, data)
    
    if result:
        return jsonify({'success': True, 'contact': result.to_dict()})
    return jsonify({'success': False, 'error': 'Contact not found'}), 404

@enterprise_bp.route('/api/crm/contacts/<contact_id>', methods=['DELETE'])
def delete_crm_contact(contact_id):
    """Delete a CRM contact"""
    if crm.delete_contact(contact_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Contact not found'}), 404

@enterprise_bp.route('/api/crm/contacts/<contact_id>/interactions', methods=['GET'])
def get_contact_interactions(contact_id):
    """Get interactions for a contact"""
    interactions = crm.get_contact_interactions(contact_id)
    return jsonify({
        'success': True,
        'interactions': [i.to_dict() for i in interactions]
    })

@enterprise_bp.route('/api/crm/interactions', methods=['POST'])
def add_interaction():
    """Add an interaction"""
    data = request.json
    
    interaction = Interaction(
        id=str(uuid.uuid4()),
        contact_id=data.get('contact_id', ''),
        type=InteractionType(data.get('type', 'note')),
        date=datetime.fromisoformat(data.get('date', datetime.now().isoformat())),
        summary=data.get('summary', ''),
        notes=data.get('notes', ''),
        outcome=data.get('outcome', ''),
        follow_up_needed=data.get('follow_up_needed', False),
        follow_up_date=datetime.fromisoformat(data['follow_up_date']) if data.get('follow_up_date') else None
    )
    
    result = crm.add_interaction(interaction)
    return jsonify({'success': True, 'interaction': result.to_dict()})

@enterprise_bp.route('/api/crm/pipeline', methods=['GET'])
def get_pipeline_summary():
    """Get pipeline summary"""
    summary = crm.get_pipeline_summary()
    stages = []
    for stage in PipelineStage:
        stages.append({
            'id': stage.value,
            'label': stage.label,
            'color': stage.color,
            'count': summary.get(stage.value, 0)
        })
    return jsonify({'success': True, 'stages': stages, 'summary': summary})

# ============================================================================
# NCAA CALENDAR ROUTES
# ============================================================================
# REMINDERS ROUTES
# ============================================================================

@enterprise_bp.route('/api/reminders', methods=['GET'])
def get_reminders():
    """Get reminders"""
    status = request.args.get('status', 'active')  # active, overdue, today, week
    school = request.args.get('school')
    
    if status == 'overdue':
        items = reminders.get_overdue()
    elif status == 'today':
        items = reminders.get_due_today()
    elif status == 'week':
        items = reminders.get_due_this_week()
    elif school:
        items = reminders.get_by_school(school)
    else:
        items = reminders.get_active_reminders()
    
    return jsonify({
        'success': True,
        'reminders': [r.to_dict() for r in items],
        'count': len(items)
    })

@enterprise_bp.route('/api/reminders', methods=['POST'])
def add_reminder():
    """Add a reminder"""
    data = request.json
    
    reminder = Reminder(
        id=data.get('id', str(uuid.uuid4())),
        title=data.get('title', ''),
        reminder_type=ReminderType(data.get('type', 'custom')),
        due_date=datetime.fromisoformat(data.get('due_date', datetime.now().isoformat())),
        school_name=data.get('school_name', ''),
        coach_name=data.get('coach_name', ''),
        contact_id=data.get('contact_id', ''),
        notes=data.get('notes', ''),
        priority=ReminderPriority(data.get('priority', 2)),
        recurring=data.get('recurring', False),
        recurring_days=data.get('recurring_days', 0)
    )
    
    result = reminders.add_reminder(reminder)
    return jsonify({'success': True, 'reminder': result.to_dict()})

@enterprise_bp.route('/api/reminders/<reminder_id>', methods=['PUT'])
def update_reminder(reminder_id):
    """Update a reminder"""
    data = request.json
    result = reminders.update_reminder(reminder_id, data)
    
    if result:
        return jsonify({'success': True, 'reminder': result.to_dict()})
    return jsonify({'success': False, 'error': 'Reminder not found'}), 404

@enterprise_bp.route('/api/reminders/<reminder_id>/complete', methods=['POST'])
def complete_reminder(reminder_id):
    """Complete a reminder"""
    result = reminders.complete_reminder(reminder_id)
    if result:
        return jsonify({'success': True, 'reminder': result.to_dict()})
    return jsonify({'success': False, 'error': 'Reminder not found'}), 404

@enterprise_bp.route('/api/reminders/<reminder_id>/snooze', methods=['POST'])
def snooze_reminder(reminder_id):
    """Snooze a reminder"""
    hours = request.json.get('hours', 24)
    result = reminders.snooze_reminder(reminder_id, hours)
    if result:
        return jsonify({'success': True, 'reminder': result.to_dict()})
    return jsonify({'success': False, 'error': 'Reminder not found'}), 404

@enterprise_bp.route('/api/reminders/<reminder_id>', methods=['DELETE'])
def delete_reminder(reminder_id):
    """Delete a reminder"""
    if reminders.delete_reminder(reminder_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Reminder not found'}), 404

@enterprise_bp.route('/api/reminders/dashboard', methods=['GET'])
def get_reminders_dashboard():
    """Get reminders dashboard data"""
    return jsonify({'success': True, 'data': reminders.get_dashboard_data()})

# ============================================================================
# EXPANDED SCHOOLS ROUTES
# ============================================================================

@enterprise_bp.route('/api/schools/expanded', methods=['GET'])
def get_expanded_schools():
    """Get expanded schools database"""
    division = request.args.get('division')
    conference = request.args.get('conference')
    state = request.args.get('state')
    
    schools = EXPANDED_SCHOOLS
    
    if division:
        schools = [s for s in schools if s.get('division', '').upper() == division.upper()]
    if conference:
        schools = [s for s in schools if conference.lower() in s.get('conference', '').lower()]
    if state:
        schools = [s for s in schools if s.get('state', '').upper() == state.upper()]
    
    return jsonify({
        'success': True,
        'schools': schools,
        'count': len(schools)
    })

@enterprise_bp.route('/api/schools/all', methods=['GET'])
def get_all_schools_combined():
    """Get all schools (base + expanded)"""
    try:
        all_schools = get_all_schools()
    except:
        all_schools = EXPANDED_SCHOOLS
    
    division = request.args.get('division')
    conference = request.args.get('conference')
    state = request.args.get('state')
    
    if division:
        all_schools = [s for s in all_schools if s.get('division', '').upper() == division.upper()]
    if conference:
        all_schools = [s for s in all_schools if conference.lower() in s.get('conference', '').lower()]
    if state:
        all_schools = [s for s in all_schools if s.get('state', '').upper() == state.upper()]
    
    return jsonify({
        'success': True,
        'schools': all_schools,
        'count': len(all_schools)
    })

# ============================================================================
# REPORTS ROUTES
# ============================================================================

@enterprise_bp.route('/api/reports/athlete', methods=['POST'])
def generate_athlete_report():
    """Generate athlete one-pager"""
    athlete = request.json
    path = reports.generate_athlete_one_pager(athlete)
    
    return jsonify({
        'success': True,
        'path': path,
        'filename': os.path.basename(path)
    })

@enterprise_bp.route('/api/reports/recruitment', methods=['POST'])
def generate_recruitment_report():
    """Generate recruitment report"""
    data = request.json
    contacts = data.get('contacts', [])
    athlete_name = data.get('athlete_name', 'Athlete')
    
    path = reports.generate_recruitment_report(contacts, athlete_name)
    
    return jsonify({
        'success': True,
        'path': path,
        'filename': os.path.basename(path)
    })

# ============================================================================
# SETUP WIZARD DATA
# ============================================================================

@enterprise_bp.route('/api/setup/status', methods=['GET'])
def get_setup_status():
    """Get setup wizard status"""
    return jsonify({
        'success': True,
        'steps': [
            {'id': 'profile', 'name': 'Athlete Profile', 'description': 'Enter your basic information'},
            {'id': 'schools', 'name': 'Select Schools', 'description': 'Choose schools to target'},
            {'id': 'email', 'name': 'Email Setup', 'description': 'Configure email settings'},
            {'id': 'templates', 'name': 'Templates', 'description': 'Customize email templates'},
        ]
    })

# ============================================================================
# HELP DOCS
# ============================================================================

@enterprise_bp.route('/api/help/topics', methods=['GET'])
def get_help_topics():
    """Get help documentation topics"""
    return jsonify({
        'success': True,
        'topics': [
            {
                'id': 'getting-started',
                'title': 'Getting Started',
                'content': '''Welcome to Coach Outreach Pro! This guide will help you get started.

1. **Complete Your Profile**: Add your athletic information, stats, and contact details.
2. **Select Schools**: Browse and add schools to your target list.
3. **Set Up Email**: Configure Gmail integration for sending emails.
4. **Create Templates**: Customize your recruiting emails.
5. **Start Reaching Out**: Send personalized emails to coaches.'''
            },
            {
                'id': 'email-setup',
                'title': 'Email Configuration',
                'content': '''To send emails, you need to set up Gmail integration:

1. Use a Gmail account
2. Enable 2-Factor Authentication
3. Generate an App Password at myaccount.google.com
4. Enter your email and app password in Settings

**Important**: App passwords are 16 characters with no spaces.'''
            },
            {
                'id': 'school-search',
                'title': 'Finding Schools',
                'content': '''Use natural language to search for schools:

- "SEC schools" - Find all SEC conference schools
- "D2 schools in Texas" - Division 2 schools in Texas
- "Small private schools" - Filter by size and type
- "FCS programs" - FCS division schools

You can combine filters: "Big Ten public schools"'''
            },
            {
                'id': 'crm',
                'title': 'Using the CRM',
                'content': '''The CRM helps you track your recruiting relationships:

**Pipeline Stages:**
- Prospect: Initial target
- Contacted: Email sent
- Interested: Coach responded positively
- Evaluating: Active evaluation
- Verbal Offer: Received verbal offer
- Committed: Verbally committed
- Signed: Signed NLI

Track interactions, notes, and follow-up reminders for each contact.'''
            }
        ]
    })

# ============================================================================
# TEMPLATE ROUTES
# ============================================================================

@enterprise_bp.route('/api/templates/prebuilt', methods=['GET'])
def get_prebuilt_templates():
    """Get all pre-built templates"""
    from .templates import get_template_manager
    manager = get_template_manager()
    return jsonify({
        'success': True,
        'templates': manager.get_all_templates()
    })

@enterprise_bp.route('/api/templates/preview', methods=['POST'])
def preview_template():
    """Preview a template with variables"""
    from .templates import get_template_manager
    data = request.json
    template_id = data.get('template_id')
    variables = data.get('variables', {})
    
    manager = get_template_manager()
    result = manager.preview_template(template_id, variables)
    
    if result:
        return jsonify({'success': True, 'preview': result})
    return jsonify({'success': False, 'error': 'Template not found'}), 404

@enterprise_bp.route('/api/templates/random', methods=['POST'])
def get_random_template():
    """Get a random template for a coach type"""
    from .templates import get_random_template_for_coach
    data = request.json
    coach_type = data.get('coach_type', 'rc')
    school = data.get('school', '')
    
    template = get_random_template_for_coach(coach_type, school)
    return jsonify({
        'success': True,
        'template': {
            'id': template.id,
            'name': template.name,
            'style': template.style,
            'subject': template.subject,
            'body': template.body
        }
    })

# ============================================================================
# FOLLOW-UP ROUTES
# ============================================================================

@enterprise_bp.route('/api/followups', methods=['GET'])
def get_followups():
    """Get follow-ups"""
    from .followups import get_followup_manager
    manager = get_followup_manager(DATA_DIR)
    
    status = request.args.get('status', 'due')  # due, upcoming, all
    
    if status == 'due':
        items = manager.get_due_followups()
    elif status == 'upcoming':
        days = int(request.args.get('days', 7))
        items = manager.get_upcoming_followups(days)
    elif status == 'overdue':
        items = manager.get_overdue_followups()
    else:
        items = list(manager.followups.values())
    
    return jsonify({
        'success': True,
        'followups': [f.to_dict() for f in items],
        'count': len(items)
    })

@enterprise_bp.route('/api/followups/dashboard', methods=['GET'])
def get_followups_dashboard():
    """Get follow-up dashboard data"""
    from .followups import get_followup_manager
    manager = get_followup_manager(DATA_DIR)
    return jsonify({
        'success': True,
        'data': manager.get_dashboard_data()
    })

@enterprise_bp.route('/api/followups/<followup_id>/sent', methods=['POST'])
def mark_followup_sent(followup_id):
    """Mark follow-up as sent"""
    from .followups import get_followup_manager
    manager = get_followup_manager(DATA_DIR)
    result = manager.mark_followup_sent(followup_id)
    
    if result:
        return jsonify({'success': True, 'followup': result.to_dict()})
    return jsonify({'success': False, 'error': 'Follow-up not found'}), 404

@enterprise_bp.route('/api/followups/<followup_id>/skip', methods=['POST'])
def skip_followup(followup_id):
    """Skip a follow-up"""
    from .followups import get_followup_manager
    manager = get_followup_manager(DATA_DIR)
    result = manager.skip_followup(followup_id)
    
    if result:
        return jsonify({'success': True, 'followup': result.to_dict()})
    return jsonify({'success': False, 'error': 'Follow-up not found'}), 404

@enterprise_bp.route('/api/followups/<followup_id>/snooze', methods=['POST'])
def snooze_followup(followup_id):
    """Snooze a follow-up"""
    from .followups import get_followup_manager
    days = request.json.get('days', 3)
    manager = get_followup_manager(DATA_DIR)
    result = manager.snooze_followup(followup_id, days)
    
    if result:
        return jsonify({'success': True, 'followup': result.to_dict()})
    return jsonify({'success': False, 'error': 'Follow-up not found'}), 404

@enterprise_bp.route('/api/followups/response', methods=['POST'])
def mark_response_received():
    """Mark that a response was received"""
    from .followups import get_followup_manager
    data = request.json
    manager = get_followup_manager(DATA_DIR)
    
    if 'email_id' in data:
        result = manager.mark_response_received(
            data['email_id'],
            data.get('status', 'responded'),
            data.get('notes', '')
        )
        if result:
            return jsonify({'success': True, 'email': result.to_dict()})
    elif 'coach_email' in data:
        results = manager.mark_response_by_coach(
            data['coach_email'],
            data.get('status', 'responded')
        )
        return jsonify({
            'success': True,
            'updated': len(results),
            'emails': [r.to_dict() for r in results]
        })
    
    return jsonify({'success': False, 'error': 'Email not found'}), 404

@enterprise_bp.route('/api/followups/config', methods=['GET', 'POST'])
def followup_config():
    """Get or update follow-up configuration"""
    from .followups import get_followup_manager
    manager = get_followup_manager(DATA_DIR)
    
    if request.method == 'POST':
        data = request.json
        manager.update_config(**data)
    
    return jsonify({
        'success': True,
        'config': manager.config.to_dict()
    })


@enterprise_bp.route('/api/followups/<followup_id>/send', methods=['POST'])
def send_single_followup(followup_id):
    """Send a single follow-up email"""
    from .followups import get_followup_manager
    from .templates import get_template_manager
    
    manager = get_followup_manager(DATA_DIR)
    
    # Find the follow-up
    followup = None
    for f in manager.followups:
        if f.id == followup_id:
            followup = f
            break
    
    if not followup:
        return jsonify({'success': False, 'error': 'Follow-up not found'}), 404
    
    # Find the original email record
    email_record = None
    for e in manager.email_records:
        if e.id == followup.email_id:
            email_record = e
            break
    
    if not email_record:
        return jsonify({'success': False, 'error': 'Original email record not found'}), 404
    
    # Get the follow-up template
    template_mgr = get_template_manager()
    template = template_mgr.get_followup_template(followup.followup_number)
    
    if not template:
        return jsonify({'success': False, 'error': 'Follow-up template not found'}), 404
    
    # Send the email
    try:
        from pathlib import Path
        import json
        
        # Load settings to get email config
        config_file = Path.home() / '.coach_outreach' / 'settings.json'
        if not config_file.exists():
            return jsonify({'success': False, 'error': 'Email not configured'}), 400
        
        with open(config_file) as f:
            settings = json.load(f)
        
        email_addr = settings.get('email', {}).get('email_address', '')
        password = settings.get('email', {}).get('app_password', '')
        athlete = settings.get('athlete', {})
        
        if not email_addr or not password:
            return jsonify({'success': False, 'error': 'Email credentials not configured'}), 400
        
        # Build variables for template
        variables = {
            'coach_name': email_record.coach_name.split()[-1] if email_record.coach_name else 'Coach',
            'school': email_record.school,
            'athlete_name': athlete.get('name', ''),
            'position': athlete.get('positions', ''),
            'hudl_link': athlete.get('highlight_url', ''),
        }
        
        # Render template
        subject = template.render_subject(variables)
        body = template.render_body(variables)
        
        # Send via SMTP
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        msg = MIMEMultipart()
        msg['From'] = email_addr
        msg['To'] = email_record.coach_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(email_addr, password)
            server.sendmail(email_addr, email_record.coach_email, msg.as_string())
        
        # Mark as sent
        manager.mark_followup_sent(followup_id)
        
        return jsonify({
            'success': True,
            'school': email_record.school,
            'email': email_record.coach_email
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@enterprise_bp.route('/api/followups/send-due', methods=['POST'])
def send_due_followups():
    """Send all due follow-up emails"""
    from .followups import get_followup_manager
    
    manager = get_followup_manager(DATA_DIR)
    due_followups = manager.get_due_followups()
    
    sent = 0
    errors = []
    
    for followup in due_followups:
        try:
            # Use the single send endpoint logic
            from flask import current_app
            with current_app.test_client() as client:
                resp = client.post(f'/api/followups/{followup.id}/send')
                if resp.status_code == 200:
                    sent += 1
                else:
                    errors.append(f"{followup.id}: {resp.get_json().get('error', 'Unknown error')}")
        except Exception as e:
            errors.append(f"{followup.id}: {str(e)}")
    
    return jsonify({
        'success': True,
        'sent': sent,
        'errors': errors,
        'total_due': len(due_followups)
    })


@enterprise_bp.route('/api/followups/send-next-round', methods=['POST'])
def send_next_round():
    """Send ONE follow-up per coach - the next due one for each coach who hasn't responded"""
    from .followups import get_followup_manager, FollowUpStatus
    from .templates import get_template_manager
    from pathlib import Path
    import json
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    manager = get_followup_manager(DATA_DIR)
    
    # Get all pending/due followups
    all_followups = [f for f in manager.followups 
                     if f.status in [FollowUpStatus.SCHEDULED, FollowUpStatus.DUE, FollowUpStatus.OVERDUE]]
    
    if not all_followups:
        return jsonify({'success': True, 'sent': 0, 'message': 'No follow-ups to send'})
    
    # Group by coach email - only get the NEXT (lowest number) follow-up for each coach
    by_coach = {}
    for f in all_followups:
        # Find the email record to get coach email
        email_record = next((e for e in manager.email_records if e.id == f.email_id), None)
        if not email_record:
            continue
        
        coach_email = email_record.coach_email
        if coach_email not in by_coach or f.followup_number < by_coach[coach_email]['followup'].followup_number:
            by_coach[coach_email] = {'followup': f, 'record': email_record}
    
    # Load settings
    config_file = Path.home() / '.coach_outreach' / 'settings.json'
    if not config_file.exists():
        return jsonify({'success': False, 'error': 'Email not configured'}), 400
    
    with open(config_file) as file:
        settings = json.load(file)
    
    email_addr = settings.get('email', {}).get('email_address', '')
    password = settings.get('email', {}).get('app_password', '')
    athlete = settings.get('athlete', {})
    
    if not email_addr or not password:
        return jsonify({'success': False, 'error': 'Email credentials not configured'}), 400
    
    template_mgr = get_template_manager()
    sent = 0
    errors = []
    followup_number = None
    
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(email_addr, password)
        
        for coach_email, data in by_coach.items():
            followup = data['followup']
            email_record = data['record']
            followup_number = followup.followup_number
            
            try:
                # Get template
                template = template_mgr.get_followup_template(followup.followup_number)
                if not template:
                    errors.append(f"{email_record.school}: No template for follow-up #{followup.followup_number}")
                    continue
                
                # Build variables
                variables = {
                    'coach_name': email_record.coach_name.split()[-1] if email_record.coach_name else 'Coach',
                    'school': email_record.school,
                    'athlete_name': athlete.get('name', ''),
                    'position': athlete.get('positions', ''),
                    'hudl_link': athlete.get('highlight_url', ''),
                }
                
                # Render template
                subject = template.render_subject(variables)
                body = template.render_body(variables)
                
                # Send
                msg = MIMEMultipart()
                msg['From'] = email_addr
                msg['To'] = coach_email
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'plain'))
                
                server.sendmail(email_addr, coach_email, msg.as_string())
                
                # Mark as sent
                manager.mark_followup_sent(followup.id)
                sent += 1
                
            except Exception as e:
                errors.append(f"{email_record.school}: {str(e)}")
        
        server.quit()
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'SMTP error: {str(e)}'}), 500
    
    return jsonify({
        'success': True,
        'sent': sent,
        'followup_number': followup_number,
        'errors': errors,
        'total_coaches': len(by_coach)
    })


# ============================================================================
# TWITTER SCRAPER ROUTES
# ============================================================================

@enterprise_bp.route('/api/twitter/search', methods=['POST'])
def search_twitter_handle():
    """Search for a coach's Twitter handle via Google"""
    from .twitter_google_scraper import find_coach_twitter
    data = request.json
    
    coach_name = data.get('coach_name', '')
    school = data.get('school', '')
    title = data.get('title', '')
    
    if not coach_name or not school:
        return jsonify({'success': False, 'error': 'coach_name and school required'}), 400
    
    handle = find_coach_twitter(coach_name, school, title)
    
    return jsonify({
        'success': True,
        'handle': handle,
        'found': handle is not None
    })

@enterprise_bp.route('/api/twitter/search-batch', methods=['POST'])
def search_twitter_batch():
    """Search for multiple coaches' Twitter handles"""
    from .twitter_google_scraper import GoogleTwitterScraper
    data = request.json
    coaches = data.get('coaches', [])
    
    if not coaches:
        return jsonify({'success': False, 'error': 'coaches list required'}), 400
    
    scraper = GoogleTwitterScraper()
    results = scraper.find_handles_batch(coaches)
    
    return jsonify({
        'success': True,
        'results': results,
        'found': len(results),
        'total': len(coaches)
    })
