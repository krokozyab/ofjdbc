# Oracle Fusion WSDL JDBC Driver

A read‑only JDBC driver that lets you run SQL queries against Oracle Fusion.
This minimal viable implementation works seamlessly in mature IDEs 
DBeaver, DbVisualizer and IntelliJ. 
And it isn’t just for Java—it can be used from any JVM‑hosted language to build custom data pipelines.

---

## 📄 Table of Contents

- [🚀 Features](#-features)
- [🛠 Prerequisites](#-prerequisites)
- [📝 Installation](#-installation)
- [⚙️ Configuration](#-configuration)
- [📜 Logging](#-logging)
- [📫 Contact](#-contact)
- [📝 License](#-license)
- [📚 Additional Resources](#-additional-resources)

---

## 🚀 Features

- **Minimalist Design:** A read‑only, no‑frills JDBC driver for Oracle Fusion reporting via WSDL.
- **IDE Integration:** Run SQL queries directly from your favorite IDE—DbVisualizer, IntelliJ, etc.—without the extra overhead.
DBeaver
- ![dbeaver](pics/dbeaver.jpg)
DBVisualizer 
- ![dbvisualizer](pics/dbvisualizer.jpg)

- **JVM-Hosted Flexibility:** Use this driver not only from Java but from any JVM‑hosted language for building custom data pipelines.
- **Easy to Configure:** Simple setup by providing your connection URL and credentials.
- **Lightweight & Focused:** Designed to provide only the essential functionality needed to retrieve report data.

---

## 🛠 Prerequisites

Before using this driver, ensure you have the following:

- **Oracle Fusion Access:** Valid credentials with access to Oracle Fusion reporting (via WSDL).
- **JDK/JRE:** A Java 8 (or later) runtime installed on your machine.
- **Your IDE of Choice:** DbVisualizer, IntelliJ IDEA, or any other mature development environment.

---

## 📝 Installation

1. **Download the Driver:**

   Download the latest version of the driver from the link below:

   [Download Driver and Instructions](https://objectstorage.us-ashburn-1.oraclecloud.com/p/wz55enC105qvFt3aBm4WsFrFy9O-wiUbWKt_QbUs_-ArviwHHvcQYggaIgN_DURD/n/idkmipa5fqwx/b/orafusjdbc/o/ofjdbc.zip)

2. **Add to Your Project:**

   Add the JAR to your project’s classpath. (If you’re using an IDE, simply add it as a library.)

3. **Register the Driver:**

   The driver automatically registers itself via its companion object. You may also register it manually if desired.

---

## ⚙️ Configuration

Use a JDBC URL of the form:


