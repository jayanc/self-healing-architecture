import smtplib
import json
from email.mime.text import MIMEText
from typing import Dict, Any, Optional, List

from ..core.interfaces import AbstractAlertManager # Use relative import
from ..core.config_manager import ConfigManager, ConfigManagerError
from ..services.loggers import ConsoleLogger # For fallback logging

class EmailAlertManager(AbstractAlertManager):
    """
    Manages sending alerts via email using smtplib.
    """

    def __init__(self, config_manager: ConfigManager, fallback_logger: Optional[AbstractLogger] = None):
        """
        Initializes the EmailAlertManager.

        Args:
            config_manager: Instance of ConfigManager to fetch email settings.
            fallback_logger: Logger to use if alerting or config fails. Defaults to ConsoleLogger.
        """
        self._config_manager = config_manager
        self._fallback_logger = fallback_logger if fallback_logger else ConsoleLogger()
        self._is_configured = False

        try:
            alert_config = self._config_manager.get_alerting_config()
            email_provider_config = alert_config.get("providers", {}).get("email", {})

            if not email_provider_config:
                self._fallback_logger.log_warning("Email provider configuration not found in alerting settings. EmailAlertManager will be disabled.", {"alerter_init": True})
                return

            self.smtp_server = email_provider_config.get("smtp_server")
            self.smtp_port = email_provider_config.get("smtp_port", 587) # Default to 587 if not specified
            self.sender_email = email_provider_config.get("default_sender")
            self.default_recipients = email_provider_config.get("default_recipients", [])
            self.critical_recipients = email_provider_config.get("critical_recipients", [])
            
            # Optional SMTP credentials (secrets management should be used in production)
            self.smtp_user_secret = email_provider_config.get("smtp_user_secret")
            self.smtp_password_secret = email_provider_config.get("smtp_password_secret")
            
            # Basic validation
            if not self.smtp_server:
                raise ConfigManagerError("SMTP server ('smtp_server') not configured for email alerts.")
            if not self.sender_email:
                raise ConfigManagerError("Sender email ('default_sender') not configured for email alerts.")
            if not self.default_recipients and not self.critical_recipients:
                self._fallback_logger.log_warning("No default or critical recipients configured for email alerts. Alerts may not be sent.", {"alerter_init": True})
            
            self._is_configured = True
            self._fallback_logger.log_info("EmailAlertManager initialized.", 
                                         {"smtp_server": self.smtp_server, "sender": self.sender_email})

        except ConfigManagerError as e:
            self._fallback_logger.log_error(f"Failed to initialize EmailAlertManager due to ConfigManagerError: {e}", e, {"alerter_init": True})
        except Exception as e:
            self._fallback_logger.log_error(f"An unexpected error occurred during EmailAlertManager initialization: {e}", e, {"alerter_init": True})

    def _get_smtp_credentials(self) -> tuple[Optional[str], Optional[str]]:
        """
        Placeholder for fetching SMTP credentials, possibly from a secrets manager.
        For now, it assumes they might be directly in config or environment variables.
        In a real scenario, this would integrate with a secrets management service.
        """
        # This is a simplified example. In production, use a secrets manager.
        # e.g., self._config_manager.get_secret(self.smtp_user_secret)
        user = None
        password = None
        if self.smtp_user_secret: # Assuming these directly hold values for now, not secret IDs
            user = self.smtp_user_secret 
        if self.smtp_password_secret:
            password = self.smtp_password_secret
        
        # If actual secret IDs were provided, you'd fetch them here.
        # Example:
        # if self.smtp_user_secret_id: user = self._config_manager.get_secret_value(self.smtp_user_secret_id)
        # if self.smtp_password_secret_id: password = self._config_manager.get_secret_value(self.smtp_password_secret_id)
        return user, password


    def send_alert(self, subject: str, body: str, severity: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Sends an alert via email.

        Args:
            subject: The subject of the email alert.
            body: The main content of the email alert.
            severity: Severity of the alert (e.g., 'INFO', 'WARNING', 'CRITICAL').
                      This can influence the recipient list.
            details: Optional dictionary with additional details to include in the email body.
        """
        if not self._is_configured:
            self._fallback_logger.log_error("EmailAlertManager is not configured. Cannot send alert.", 
                                          {"subject": subject, "severity": severity})
            return

        recipients: List[str] = []
        if severity.upper() == "CRITICAL" and self.critical_recipients:
            recipients.extend(self.critical_recipients)
        
        # If no critical recipients for a critical alert, or for non-critical, use default.
        # Also, ensure default_recipients are always added if defined, or make it more specific.
        if not recipients or severity.upper() != "CRITICAL":
            if isinstance(self.default_recipients, list):
                 recipients.extend(self.default_recipients)
            elif isinstance(self.default_recipients, str): # if it's a single string email
                 recipients.append(self.default_recipients)

        if not recipients:
            self._fallback_logger.log_warning("No recipients determined for email alert.", 
                                            {"subject": subject, "severity": severity})
            return
        
        # Remove duplicates
        final_recipients = list(set(recipients))

        email_body = f"Severity: {severity.upper()}\n\n{body}\n\n"
        if details:
            try:
                details_formatted = json.dumps(details, indent=2, sort_keys=True, default=str)
                email_body += f"Additional Details:\n{details_formatted}\n"
            except TypeError:
                email_body += f"Additional Details: (unserializable content)\n{str(details)}\n"
        
        msg = MIMEText(email_body)
        msg['Subject'] = f"[SelfHealingAlert:{severity.upper()}] {subject}"
        msg['From'] = self.sender_email
        msg['To'] = ", ".join(final_recipients)

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                # server.set_debuglevel(1) # Uncomment for SMTP debugging
                server.ehlo()
                # Note: TLS/SSL handling might need to be more robust depending on SMTP server requirements
                if server.has_extn('STARTTLS'):
                    server.starttls()
                    server.ehlo() # Re-EHLO after STARTTLS

                smtp_user, smtp_password = self._get_smtp_credentials()
                if smtp_user and smtp_password:
                    try:
                        server.login(smtp_user, smtp_password)
                    except smtplib.SMTPAuthenticationError as e:
                        self._fallback_logger.log_error(f"SMTP authentication failed for user {smtp_user}.", e, 
                                                      {"subject": subject, "recipients": final_recipients})
                        return # Stop if auth fails
                    except smtplib.SMTPException as e:
                        self._fallback_logger.log_error(f"SMTP login error for user {smtp_user}: {e}", e,
                                                      {"subject": subject, "recipients": final_recipients})
                        return


                server.sendmail(self.sender_email, final_recipients, msg.as_string())
                self._fallback_logger.log_info(f"Email alert sent successfully to {', '.join(final_recipients)}.", 
                                             {"subject": subject})
        except smtplib.SMTPConnectError as e:
            self._fallback_logger.log_error(f"Failed to connect to SMTP server {self.smtp_server}:{self.smtp_port}.", e, 
                                          {"subject": subject, "recipients": final_recipients})
        except smtplib.SMTPRecipientsRefused as e:
            self._fallback_logger.log_error(f"All recipient addresses refused by SMTP server.", e, 
                                          {"subject": subject, "recipients": final_recipients, "refused_recipients": e.recipients})
        except smtplib.SMTPSenderRefused as e:
             self._fallback_logger.log_error(f"Sender address {self.sender_email} refused by SMTP server.", e, 
                                          {"subject": subject, "recipients": final_recipients})
        except smtplib.SMTPDataError as e:
            self._fallback_logger.log_error(f"SMTP server refused message data.", e, 
                                          {"subject": subject, "recipients": final_recipients})
        except Exception as e:
            self._fallback_logger.log_error(f"An unexpected error occurred while sending email alert: {e}", e, 
                                          {"subject": subject, "recipients": final_recipients})

# Example usage (requires ConfigManager and policy_config.yaml setup):
# if __name__ == '__main__':
#     from ..core.config_manager import ConfigManager # Adjust import if running directly
#     # Assuming config_manager.py is in src/core and this is src/services
#     # and policy_config.yaml is in config/ at the project root.
#     # This pathing needs to be robust based on execution context.
#     project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#     cfg_path = os.path.join(project_root, "config", "policy_config.yaml")
    
#     try:
#         cfg_manager = ConfigManager(config_path=cfg_path)
#         email_alerter = EmailAlertManager(cfg_manager)
        
#         if email_alerter._is_configured: # Check if it was configured successfully
#             email_alerter.send_alert(
#                 subject="Test Critical Alert from SelfHealingFramework",
#                 body="This is a test of the EmailAlertManager for a CRITICAL event.",
#                 severity="CRITICAL",
#                 details={"job_id": "job_12345", "error_code": "BQ_TIMEOUT", "attempted_retries": 3}
#             )
#             email_alerter.send_alert(
#                 subject="Test Info Alert from SelfHealingFramework",
#                 body="This is a test of the EmailAlertManager for an INFO event.",
#                 severity="INFO",
#                 details={"task_name": "data_validation_silver", "status": "completed_with_warnings"}
#             )
#         else:
#             print("EmailAlertManager was not configured, so no test alerts sent.")
            
#     except ConfigManagerError as e:
#         print(f"ConfigManagerError during example: {e}")
#     except Exception as e:
#         print(f"Unexpected error during example: {e}")
