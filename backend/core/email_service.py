"""
Email service for sending authentication-related emails

This module provides email functionality for:
- Password reset emails
- Email verification emails  
- Welcome emails

Uses SendGrid API for email delivery. Can be adapted for other providers.
"""

import os
import logging
from typing import Optional
import base64
import json

logger = logging.getLogger(__name__)

# Configuration from environment variables
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'noreply@approachnote.com')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'approachnote://auth')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.approachnote.com')

# Check if SendGrid is configured
SENDGRID_CONFIGURED = bool(SENDGRID_API_KEY)

if not SENDGRID_CONFIGURED:
    logger.warning("SendGrid not configured - emails will be logged but not sent")
    logger.warning("Set SENDGRID_API_KEY environment variable to enable email sending")


def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """
    Send email using SendGrid REST API directly (avoids dependency issues)
    
    Args:
        to_email: Recipient email address
        subject: Email subject line
        html_content: HTML content of the email
        
    Returns:
        True if email sent successfully, False otherwise
    """
    # If SendGrid not configured, log the email instead
    if not SENDGRID_CONFIGURED:
        logger.info(f"[EMAIL NOT SENT - No SendGrid API key] To: {to_email}")
        logger.info(f"[EMAIL] Subject: {subject}")
        logger.info(f"[EMAIL] Content: {html_content[:200]}...")
        return False
    
    try:
        import requests
        
        # Use SendGrid REST API directly
        url = "https://api.sendgrid.com/v3/mail/send"
        
        headers = {
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "personalizations": [
                {
                    "to": [{"email": to_email}],
                    "subject": subject
                }
            ],
            "from": {"email": FROM_EMAIL},
            "content": [
                {
                    "type": "text/html",
                    "value": html_content
                }
            ]
        }
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code in [200, 202]:
            logger.info(f"Email sent to {to_email}: {response.status_code}")
            return True
        else:
            logger.error(f"Email send failed: {response.status_code} - {response.text}")
            return False
        
    except Exception as e:
        logger.error(f"Email send failed to {to_email}: {e}")
        return False


def send_verification_email(email: str, token: str) -> bool:
    """
    Send email verification link
    
    Args:
        email: User's email address
        token: Verification token
        
    Returns:
        True if email sent successfully, False otherwise
    """
    verify_url = f"{FRONTEND_URL}/verify-email?token={token}"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .button {{ 
                    display: inline-block; 
                    padding: 12px 24px; 
                    background-color: #4F46E5; 
                    color: white; 
                    text-decoration: none; 
                    border-radius: 6px; 
                    margin: 20px 0; 
                }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Welcome to ApproachNote!!</h2>
                <p>Please verify your email address by clicking the button below:</p>
                <a href="{verify_url}" class="button">Verify Email Address</a>
                <p>Or copy and paste this link into your browser:</p>
                <p style="word-break: break-all; color: #4F46E5;">{verify_url}</p>
                <p class="footer">
                    This link will expire in 24 hours.<br>
                    If you didn't create an account, you can safely ignore this email.
                </p>
            </div>
        </body>
    </html>
    """
    
    logger.info(f"Sending verification email to: {email}")
    return send_email(email, "Verify your ApproachNote email", html_content)


def send_password_reset_email(email: str, token: str) -> bool:
    """
    Send password reset link
    
    Args:
        email: User's email address
        token: Password reset token
        
    Returns:
        True if email sent successfully, False otherwise
    """
    reset_url = f"{FRONTEND_URL}/reset-password?token={token}"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .button {{ 
                    display: inline-block; 
                    padding: 12px 24px; 
                    background-color: #DC2626; 
                    color: white; 
                    text-decoration: none; 
                    border-radius: 6px; 
                    margin: 20px 0; 
                }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Password Reset Request</h2>
                <p>You requested to reset your password. Click the button below to proceed:</p>
                <a href="{reset_url}" class="button">Reset Password</a>
                <p>Or copy and paste this link into your browser:</p>
                <p style="word-break: break-all; color: #DC2626;">{reset_url}</p>
                <p class="footer">
                    This link will expire in 1 hour.<br>
                    If you didn't request a password reset, you can safely ignore this email.
                    Your password will not be changed unless you click the link above and create a new password.
                </p>
            </div>
        </body>
    </html>
    """
    
    logger.info(f"Sending password reset email to: {email}")
    return send_email(email, "Reset your ApproachNote password", html_content)


def send_welcome_email(email: str, display_name: Optional[str] = None) -> bool:
    """
    Send welcome email after successful registration
    
    Args:
        email: User's email address
        display_name: User's display name (optional)
        
    Returns:
        True if email sent successfully, False otherwise
    """
    greeting = f"Welcome, {display_name}!" if display_name else "Welcome!"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #4F46E5; color: white; padding: 20px; border-radius: 6px; }}
                .content {{ padding: 20px 0; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0;">ApproachNote</h1>
                </div>
                <div class="content">
                    <h2>{greeting}</h2>
                    <p>Your account has been successfully created.</p>
                    <p>You now have access to:</p>
                    <ul>
                        <li>Comprehensive jazz standards database</li>
                        <li>Curated canonical recordings</li>
                        <li>Artist biographies and discographies</li>
                        <li>Personal repertoire tracking (coming soon)</li>
                    </ul>
                    <p>Start exploring the world's most comprehensive jazz standards reference!</p>
                </div>
                <div class="footer">
                    <p>Questions? Contact us at support@approachnote.com</p>
                </div>
            </div>
        </body>
    </html>
    """
    
    logger.info(f"Sending welcome email to: {email}")
    return send_email(email, "Welcome to ApproachNote", html_content)