#!/bin/bash

# Customer Complaints ETL Pipeline - Azure Deployment Script
# This script automates the deployment of the ETL pipeline to Azure using Terraform

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to validate prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check if Azure CLI is installed
    if ! command_exists az; then
        print_error "Azure CLI is not installed. Please install it first."
        echo "Visit: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
        exit 1
    fi
    
    # Check if Terraform is installed
    if ! command_exists terraform; then
        print_error "Terraform is not installed. Please install it first."
        echo "Visit: https://www.terraform.io/downloads.html"
        exit 1
    fi
    
    # Check if logged into Azure
    if ! az account show >/dev/null 2>&1; then
        print_error "Not logged into Azure. Please run 'az login' first."
        exit 1
    fi
    
    print_success "Prerequisites check passed!"
}

# Function to validate Terraform configuration
validate_terraform() {
    print_status "Validating Terraform configuration..."
    
    if ! terraform validate; then
        print_error "Terraform configuration validation failed!"
        exit 1
    fi
    
    print_success "Terraform configuration is valid!"
}

# Function to check if terraform.tfvars exists
check_tfvars() {
    if [ ! -f "terraform.tfvars" ]; then
        print_warning "terraform.tfvars file not found!"
        print_status "Creating terraform.tfvars from example..."
        
        if [ -f "terraform.tfvars.example" ]; then
            cp terraform.tfvars.example terraform.tfvars
            print_warning "Please edit terraform.tfvars with your specific values before continuing."
            print_warning "Especially update the sql_admin_password!"
            echo ""
            echo "Press Enter to continue after editing terraform.tfvars, or Ctrl+C to exit..."
            read -r
        else
            print_error "terraform.tfvars.example not found!"
            exit 1
        fi
    fi
}

# Function to display current Azure context
show_azure_context() {
    print_status "Current Azure context:"
    echo "Subscription: $(az account show --query name -o tsv)"
    echo "Account: $(az account show --query user.name -o tsv)"
    echo ""
}

# Function to estimate costs
estimate_costs() {
    print_status "Estimated monthly costs (approximate):"
    echo "- Azure SQL Database (S1): ~$20"
    echo "- Databricks Workspace (Standard): ~$0.40/DBU"
    echo "- Storage Account: ~$5-10"
    echo "- Key Vault: ~$1"
    echo "- Total estimated: ~$30-50/month (depending on usage)"
    echo ""
    print_warning "Actual costs may vary based on usage patterns."
    echo ""
}

# Function to run Terraform plan
run_plan() {
    print_status "Running Terraform plan..."
    
    if terraform plan -out=tfplan; then
        print_success "Terraform plan completed successfully!"
        echo ""
        print_status "Review the plan above. Do you want to proceed with deployment? (y/N)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            print_status "Deployment cancelled by user."
            exit 0
        fi
    else
        print_error "Terraform plan failed!"
        exit 1
    fi
}

# Function to run Terraform apply
run_apply() {
    print_status "Deploying infrastructure..."
    
    if terraform apply tfplan; then
        print_success "Infrastructure deployed successfully!"
        
        # Clean up plan file
        rm -f tfplan
        
        # Show outputs
        echo ""
        print_status "Deployment outputs:"
        terraform output
        
    else
        print_error "Terraform apply failed!"
        exit 1
    fi
}

# Function to show post-deployment instructions
show_post_deployment() {
    echo ""
    print_success "Deployment completed successfully!"
    echo ""
    print_status "Next steps:"
    echo "1. Access your Databricks workspace using the URL from the output above"
    echo "2. Navigate to Workflows → Jobs to see your ETL pipeline"
    echo "3. Run the job manually to test the pipeline"
    echo "4. Connect to your SQL Database to query the results"
    echo ""
    print_status "For detailed instructions, see the README.md file."
    echo ""
    print_warning "Remember to monitor your Azure costs and clean up resources when no longer needed."
}

# Function to initialize Terraform
init_terraform() {
    print_status "Initializing Terraform..."
    
    if terraform init; then
        print_success "Terraform initialized successfully!"
    else
        print_error "Terraform initialization failed!"
        exit 1
    fi
}

# Function to show help
show_help() {
    echo "Customer Complaints ETL Pipeline - Azure Deployment Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -p, --plan     Run terraform plan only (no deployment)"
    echo "  -d, --destroy  Destroy the infrastructure"
    echo "  -f, --force    Skip confirmation prompts"
    echo ""
    echo "Examples:"
    echo "  $0              # Full deployment with prompts"
    echo "  $0 --plan       # Plan only, no deployment"
    echo "  $0 --destroy    # Destroy infrastructure"
    echo "  $0 --force      # Deploy without confirmation prompts"
}

# Function to destroy infrastructure
destroy_infrastructure() {
    print_warning "This will destroy ALL resources created by this Terraform configuration!"
    print_warning "This action cannot be undone!"
    echo ""
    
    if [ "$FORCE" != "true" ]; then
        print_status "Are you sure you want to destroy the infrastructure? (type 'yes' to confirm)"
        read -r response
        if [ "$response" != "yes" ]; then
            print_status "Destruction cancelled by user."
            exit 0
        fi
    fi
    
    print_status "Destroying infrastructure..."
    
    if terraform destroy -auto-approve; then
        print_success "Infrastructure destroyed successfully!"
    else
        print_error "Terraform destroy failed!"
        exit 1
    fi
}

# Main function
main() {
    # Parse command line arguments
    PLAN_ONLY=false
    DESTROY=false
    FORCE=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -p|--plan)
                PLAN_ONLY=true
                shift
                ;;
            -d|--destroy)
                DESTROY=true
                shift
                ;;
            -f|--force)
                FORCE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Change to terraform directory if not already there
    if [ ! -f "main.tf" ]; then
        if [ -d "terraform" ]; then
            cd terraform
        else
            print_error "Terraform configuration files not found!"
            print_error "Please run this script from the project root or terraform directory."
            exit 1
        fi
    fi
    
    # Show banner
    echo "=================================================="
    echo "Customer Complaints ETL Pipeline - Azure Deployment"
    echo "=================================================="
    echo ""
    
    # Check prerequisites
    check_prerequisites
    
    # Show Azure context
    show_azure_context
    
    # Handle destroy option
    if [ "$DESTROY" = true ]; then
        destroy_infrastructure
        exit 0
    fi
    
    # Check terraform.tfvars
    check_tfvars
    
    # Initialize Terraform
    init_terraform
    
    # Validate Terraform configuration
    validate_terraform
    
    # Show cost estimates
    if [ "$FORCE" != "true" ]; then
        estimate_costs
    fi
    
    # Run Terraform plan
    run_plan
    
    # If plan only, exit here
    if [ "$PLAN_ONLY" = true ]; then
        print_status "Plan completed. Use without --plan flag to deploy."
        exit 0
    fi
    
    # Run Terraform apply
    run_apply
    
    # Show post-deployment instructions
    show_post_deployment
}

# Run main function
main "$@"
