import re

def validate_email(email):
    if(re.fullmatch(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', str(email))):
        return True
    else:
        return False

def loose_validate_email(email):
    if all(x in email for x in ['@', '.']):
        return True
    else:
        return False

def vaild_gmail_check(email):
    flaging = re.search(r'@gmail\.com\b', email, re.I)
    return True if flaging else False

def return_only_valid_emails(email, loose_rules=False):
    if loose_rules:
        if loose_validate_email(email):
            return email
        else:
            return ''
    else:
        if validate_email(email):
            return email
        else:
            return ''

def fix_con_to_com(email):
    email = str(email).upper()
    if email.split('.')[-1] == 'CON':
        email = ".".join(email.split('.')[:-1]) + '.COM'
    return email

def strip_domain_from_email(email):
    domain = email.split('@')[-1]
    return domain