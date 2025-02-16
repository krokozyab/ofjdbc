# Oracle Fusion JDBC Driver

A read‑only JDBC driver that lets you run SQL queries against Oracle Fusion.
This minimal viable implementation works seamlessly in mature IDE's - DBeaver, DbVisualizer and IntelliJ. 
And it isn’t just for Java - it can be used from any JVM‑hosted language to build custom data pipelines.

---

## 📄 Table of Contents

- [🚀 Features](#-features)
- [🛠 Prerequisites](#-prerequisites)
- [📝 Installation](#-installation)
- [⚙️ Configuration](#-configuration)
- [📜 Logging](#-logging)
- [📝 TODO](#-todo)
- [📫 Contact](#-contact)
- [📝 License](#-license)
- [📚 Additional Resources](#-additional-resources)

---

## 🚀 Features
- **Your Credentials, Your Control:** Unlike many closed‑source solutions, this driver keeps your credentials under your control, ensuring peace of mind and security.
- **Minimalist Design:** A read‑only, no‑frills JDBC driver for Oracle Fusion exploring/reporting via WSDL.
- **IDE Integration:** Run SQL queries directly from your favorite IDE - DbVisualizer, IntelliJ, etc. without the extra overhead.
_**DBeaver**_
![dbeaver](pics/dbeaver.jpg)
_**DBVisualizer**_ 
![dbvisualizer](pics/dbvisualizer.jpg)
- **JVM-Hosted Flexibility:** Use this driver not only from Java but from any JVM‑hosted language for building custom data pipelines.
- **Easy to Configure:** Simple setup by providing your connection URL and credentials.

---

## 🛠 Prerequisites

Before using this driver, ensure you have the following:

- **Oracle Fusion Access:** Valid credentials with access to Oracle Fusion reporting (via WSDL).
- **JDK/JRE:** A Java 8 (or later) runtime installed on your machine.
- **Your IDE of Choice:** DBeaver, DbVisualizer, IntelliJ IDEA (or any other mature development environment I don't know and tested yet :)) .

---

## 📝 Installation

1. **Download the Driver:**

   Download the latest version of the driver from the releases of this repository linked below:

   [release driver](https://github.com/krokozyab/ofjdbc/releases/tag/initial)

2. **Add to Your Project:**

   Add the JAR to your project’s classpath. (If you’re using an IDE, simply add it as a library.)

3. **Register the Driver:**

   The driver automatically registers itself via its companion object. You may also register it manually if desired.

---

## ⚙️ Configuration

Use a JDBC URL of the form:

## 📝 TODO

This project is a minimal viable implementation, and there are several areas for future enhancement:

- **Additional JDBC Features:**
   - Support for advanced JDBC methods (scrollable ResultSets, etc.).

- **Error Handling:**
   - Improve the integration with IDE-specific features (e.g., better error messages).

- **Performance & Scalability:**
   - Optimize query pagination and fetch size management.

- **Extended Metadata:**
   - Implement additional DatabaseMetaData methods to provide richer metadata support.

- **Customizability:**
   - Explore supporting additional authentication mechanisms beyond Basic Auth.

Check back on the GitHub repository for updates and improvements as the project evolves.



