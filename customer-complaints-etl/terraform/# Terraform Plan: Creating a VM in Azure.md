# Terraform Plan: Creating a VM in Azure

This guide outlines the steps to write Terraform files for provisioning a Virtual Machine (VM) in Azure, with explanations for each step.

---

## 1. Set Up the Provider

**Explanation:**  
Terraform needs to know which cloud provider to use and how to authenticate. The provider block tells Terraform to use Azure and can reference credentials from environment variables or a service principal.

```hcl
provider "azurerm" {
  features {}
}
```

---

## 2. Create a Resource Group

**Explanation:**  
A resource group is a logical container for Azure resources. All resources for your VM will be grouped here.

```hcl
resource "azurerm_resource_group" "main" {
  name     = "example-resource-group"
  location = "East US"
}
```

---

## 3. Create a Virtual Network (VNet)

**Explanation:**  
A VNet provides network isolation and segmentation for your VM. It’s like a private network in Azure.

```hcl
resource "azurerm_virtual_network" "main" {
  name                = "example-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}
```

---

## 4. Create a Subnet

**Explanation:**  
A subnet divides your VNet into smaller segments. Your VM will be placed in this subnet.

```hcl
resource "azurerm_subnet" "main" {
  name                 = "example-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]
}
```

---

## 5. Create a Network Interface

**Explanation:**  
A network interface connects your VM to the subnet and, optionally, to the internet.

```hcl
resource "azurerm_network_interface" "main" {
  name                = "example-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.main.id
    private_ip_address_allocation = "Dynamic"
  }
}
```

---

## 6. Create a Public IP Address (Optional)

**Explanation:**  
If you want your VM to be accessible from the internet, you need a public IP.

```hcl
resource "azurerm_public_ip" "main" {
  name                = "example-public-ip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Dynamic"
}
```

Add the public IP to your network interface if needed.

---

## 7. Create a Network Security Group (Firewall)

**Explanation:**  
A Network Security Group (NSG) acts as a firewall to control inbound and outbound traffic to your VM.

```hcl
resource "azurerm_network_security_group" "main" {
  name                = "example-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}
```

Associate the NSG with your subnet or network interface.

---

## 8. Create a Virtual Machine

**Explanation:**  
This is the main resource. You specify the VM size, OS image, admin credentials, and attach the network interface.

```hcl
resource "azurerm_linux_virtual_machine" "main" {
  name                = "example-vm"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  size                = "Standard_B1s"
  admin_username      = "azureuser"

  network_interface_ids = [
    azurerm_network_interface.main.id,
  ]

  admin_ssh_key {
    username   = "azureuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }
}
```

---

## 9. Outputs (Optional)

**Explanation:**  
Outputs let you see important information after deployment, such as the public IP address.

```hcl
output "public_ip_address" {
  value = azurerm_public_ip.main.ip_address
}
```

---

## 10. Variables and tfvars (Optional)

**Explanation:**  
Use variables for flexibility and to avoid hardcoding values.

```hcl
variable "location" {
  default = "East US"
}
```

---

## Summary Table

| Step | Resource Type                        | Purpose/Explanation                                 |
|------|--------------------------------------|-----------------------------------------------------|
| 1    | provider "azurerm"                   | Set up Azure provider                               |
| 2    | azurerm_resource_group               | Logical container for resources                     |
| 3    | azurerm_virtual_network              | Private network for VM                              |
| 4    | azurerm_subnet                       | Subnet for VM                                       |
| 5    | azurerm_network_interface            | Connects VM to network                              |
| 6    | azurerm_public_ip (optional)         | Public access to VM                                 |
| 7    | azurerm_network_security_group       | Firewall for VM                                     |
| 8    | azurerm_linux_virtual_machine        | The VM itself                                       |
| 9    | output (optional)                    | Show info after apply                               |
| 10   | variable (optional)                  | Parameterize your configuration                     |

---

**Tip:**  
Write each resource in its own `.tf` file or group related resources together for clarity. Use variables for anything that might change between environments.

Let me know if you want a full example or help with a specific