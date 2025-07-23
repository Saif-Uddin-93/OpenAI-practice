"""
Mock data generator for customer complaints
"""
import json
import uuid
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from config.settings import DATA_GENERATION, ETL_CONFIG

logger = logging.getLogger(__name__)
fake = Faker()

class ComplaintDataGenerator:
    """Generates realistic mock customer complaint data"""
    
    def __init__(self):
        self.config = DATA_GENERATION
        self.etl_config = ETL_CONFIG
        
    def _generate_complaint_text(self, complaint_type, severity):
        """Generate realistic complaint text based on type and severity"""
        
        complaint_templates = {
            'Product Quality': [
                "The product I received was defective and doesn't work as advertised.",
                "Poor quality materials used in the product, broke after minimal use.",
                "Product doesn't match the description on your website.",
                "Manufacturing defect noticed immediately upon opening the package."
            ],
            'Service Issues': [
                "Customer service representative was unhelpful and rude.",
                "Long wait times and poor service quality during my recent interaction.",
                "Staff member didn't understand my issue and provided wrong information.",
                "Unprofessional behavior from your service team."
            ],
            'Billing Problems': [
                "Incorrect charges appeared on my bill without explanation.",
                "Double charged for the same service this month.",
                "Billing error that hasn't been resolved despite multiple calls.",
                "Unexpected fees added to my account without notification."
            ],
            'Delivery Issues': [
                "Package was delivered to wrong address despite correct information.",
                "Delayed delivery caused significant inconvenience.",
                "Package arrived damaged due to poor handling.",
                "Missing items from my order, only partial delivery received."
            ],
            'Technical Support': [
                "Technical issue not resolved after multiple support sessions.",
                "Software bug causing system crashes repeatedly.",
                "Unable to access my account due to technical problems.",
                "Feature not working as documented in user manual."
            ],
            'Account Management': [
                "Cannot access my account despite correct login credentials.",
                "Account settings were changed without my authorization.",
                "Unable to update my profile information through the system.",
                "Account suspension without proper notification or explanation."
            ],
            'Refund Requests': [
                "Requesting refund for unsatisfactory product received.",
                "Service didn't meet expectations, would like money back.",
                "Product returned but refund not processed yet.",
                "Charged for cancelled service, need immediate refund."
            ]
        }
        
        base_text = random.choice(complaint_templates.get(complaint_type, ["General complaint about service."]))
        
        # Add severity-specific language
        severity_modifiers = {
            'Critical': [" This is an emergency situation.", " Urgent attention required.", " Critical business impact."],
            'High': [" This needs immediate attention.", " Very serious issue.", " Major problem affecting operations."],
            'Medium': [" Please address this soon.", " Moderate inconvenience caused.", " Needs resolution."],
            'Low': [" When convenient, please look into this.", " Minor issue but worth mentioning.", " Suggestion for improvement."]
        }
        
        if severity in severity_modifiers:
            base_text += random.choice(severity_modifiers[severity])
            
        return base_text
    
    def _determine_severity(self, complaint_text):
        """Determine severity based on complaint text keywords"""
        text_lower = complaint_text.lower()
        
        for severity, keywords in self.config['severity_keywords'].items():
            if any(keyword.lower() in text_lower for keyword in keywords):
                return severity
                
        # Default severity distribution if no keywords match
        return random.choices(
            ['Critical', 'High', 'Medium', 'Low'],
            weights=[5, 15, 40, 40]  # Realistic distribution
        )[0]
    
    def _generate_customer(self):
        """Generate a single customer record"""
        return {
            'customer_uuid': str(uuid.uuid4()),
            'age_group': random.choice(self.config['age_groups']),
            'region': random.choice(self.config['regions']),
            'customer_type': random.choice(self.config['customer_types']),
            'gender': random.choice(self.config['genders'])
        }
    
    def _generate_complaint(self, customer):
        """Generate a single complaint record"""
        complaint_type = random.choice(self.config['complaint_types'])
        
        # Generate complaint text first
        severity = random.choices(
            ['Critical', 'High', 'Medium', 'Low'],
            weights=[5, 15, 40, 40]
        )[0]
        
        complaint_text = self._generate_complaint_text(complaint_type, severity)
        
        # Re-determine severity based on text (for realism)
        final_severity = self._determine_severity(complaint_text)
        
        # Generate complaint date within last 90 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        complaint_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        return {
            'complaint_uuid': str(uuid.uuid4()),
            'customer_uuid': customer['customer_uuid'],
            'complaint_type': complaint_type,
            'severity': final_severity,
            'complaint_text': complaint_text,
            'complaint_date': complaint_date.isoformat()
        }
    
    def generate_mock_data(self, num_records=None):
        """Generate complete mock dataset"""
        if num_records is None:
            num_records = self.etl_config['mock_data_size']
            
        logger.info(f"Generating {num_records} mock complaint records")
        
        customers = []
        complaints = []

        # Generate unique customers (fewer customers than complaints for realism)
        num_customers = max(1, num_records // 3)  # Each customer has ~3 complaints on average
        
        for _ in range(num_customers):
            customers.append(self._generate_customer())
        
        # Generate complaints
        for _ in range(num_records):
            customer = random.choice(customers)
            complaint = self._generate_complaint(customer)
            complaints.append(complaint)
        
        # Create combined dataset
        dataset = {
            'customers': customers,
            'complaints': complaints,
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_customers': len(customers),
                'total_complaints': len(complaints),
                'generator_version': '1.0'
            }
        }
        
        logger.info(f"Generated {len(customers)} customers and {len(complaints)} complaints")
        return dataset
    
    def save_data(self, dataset, output_path='data/mock_complaints'):
        """Save generated data to file"""
        
        output_format = self.etl_config['output_format'].lower()
        
        if output_format == 'json':
            file_path = f"{output_path}.json"
            with open(file_path, 'w') as f:
                json.dump(dataset, f, indent=2, default=str)
            logger.info(f"Data saved to {file_path}")
            
        elif output_format == 'csv':
            # Save customers and complaints as separate CSV files
            customers_df = pd.DataFrame(dataset['customers'])
            complaints_df = pd.DataFrame(dataset['complaints'])
            
            customers_file = f"{output_path}_customers.csv"
            complaints_file = f"{output_path}_complaints.csv"
            
            customers_df.to_csv(customers_file, index=False)
            complaints_df.to_csv(complaints_file, index=False)
            
            logger.info(f"Data saved to {customers_file} and {complaints_file}")
            
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        return dataset

def main():
    """Main function for testing data generation"""
    generator = ComplaintDataGenerator()
    dataset = generator.generate_mock_data(100)  # Generate 100 records for testing
    generator.save_data(dataset)
    
    print(f"Generated {len(dataset['customers'])} customers and {len(dataset['complaints'])} complaints")
    print("Sample complaint:", dataset['complaints'][0])

if __name__ == "__main__":
    main()
