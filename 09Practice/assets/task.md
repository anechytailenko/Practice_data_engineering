### Practical Task: DWH Design for **Airbnb**

---

#### **Goal**

Design a Data Warehouse schema using layered architecture and SCD Type 2.

---

#### **Task**

* Work in teams
* Draw a **DWH schema only** (no data)
* Business context: Airbnb

---

#### **Requirements**

1. Use **5 different data sources**
2. Use **3 DWH layers**:

   * `raw`
   * `stage`
   * `mart`
3. Implement **SCD Type 2** for at least **2 dimensions**
4. Show table relationships and data flow

---

#### **Suggested Data Sources (example)**

* Listings
* Hosts
* Guests
* Bookings
* Payments / Reviews / Search events

---

#### **Layer Expectations**

* **RAW**: source-aligned, no transformations
* **STAGE**: cleaned and standardized
* **MART**: star schema (facts + dimensions), SCD Type 2
---

#### **Deliverable**

* Diagram (Miro / Draw.io / similar / Marmaid)
* English table & column names
