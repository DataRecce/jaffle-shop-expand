{% docs orders_status %}
Orders can have the following statuses:
| Status | Description |
|--------|------------|
| placed | Order has been placed but not yet processed |
| shipped | Order has been shipped to the customer |
| completed | Order has been delivered and confirmed |
| return_pending | Customer has requested a return |
| returned | Return has been processed |
{% enddocs %}

{% docs customer_type %}
Customers are classified as:
- **new**: Has placed only one order
- **returning**: Has placed more than one order
{% enddocs %}

{% docs rfm_segments %}
RFM (Recency, Frequency, Monetary) segmentation scores customers on a 1-5 scale across three dimensions:
- **Recency**: How recently a customer made a purchase (5 = most recent)
- **Frequency**: How often they purchase (5 = most frequent)
- **Monetary**: How much they spend (5 = highest spend)

The combined RFM score (3-15) maps to segments:
| Score Range | Segment |
|-------------|---------|
| 13-15 | Champion |
| 10-12 | Loyal |
| 7-9 | Potential Loyalist |
| 5-6 | At Risk |
| 3-4 | Lost |
{% enddocs %}

{% docs loyalty_tiers %}
The loyalty program has four tiers based on cumulative points:
- **Bronze**: 0-499 points (entry level)
- **Silver**: 500-1,499 points (5% discount on orders)
- **Gold**: 1,500-4,999 points (10% discount, free delivery)
- **Platinum**: 5,000+ points (15% discount, free delivery, priority support)

Points are earned at a rate of 1 point per dollar spent. Tier status is recalculated monthly.
{% enddocs %}

{% docs payment_methods %}
Accepted payment methods:
- **credit_card**: Visa, Mastercard, Amex
- **debit_card**: Direct bank debit
- **gift_card**: Jaffle Shop gift cards
- **cash**: In-store cash payments
- **mobile_pay**: Apple Pay, Google Pay
{% enddocs %}

{% docs churn_risk_levels %}
Customer churn risk is classified based on behavioral signals:
- **active**: Regular ordering pattern, no signs of churn
- **watch**: Slight decline in order frequency
- **medium_risk**: Significant drop in activity, longer gap since last order
- **high_risk**: No orders in extended period, declining engagement
{% enddocs %}

{% docs store_quadrants %}
Stores are classified into a BCG-style 2x2 matrix:
- **Star**: High revenue growth + high profitability — invest and expand
- **Cash Cow**: Low growth + high profitability — maintain and optimize
- **Question Mark**: High growth + low profitability — investigate and improve
- **Dog**: Low growth + low profitability — turnaround or consider closure
{% enddocs %}

{% docs expense_classifications %}
Expenses are classified into three categories for financial reporting:
- **COGS** (Cost of Goods Sold): Direct costs of producing menu items (ingredients, packaging)
- **OpEx** (Operating Expenses): Indirect costs of running stores (rent, utilities, marketing)
- **Other**: Non-recurring or miscellaneous expenses
{% enddocs %}

{% docs invoice_status %}
Invoices progress through the following statuses:
| Status | Description |
|--------|------------|
| issued | Invoice has been generated |
| sent | Invoice has been sent to the customer |
| paid | Payment has been received in full |
| partial | Partial payment received |
| overdue | Payment is past the due date |
| cancelled | Invoice has been cancelled |
{% enddocs %}

{% docs shift_types %}
Employee shifts are categorized as:
- **morning**: Opening shift (typically 6am-2pm)
- **afternoon**: Mid-day shift (typically 10am-6pm)
- **evening**: Closing shift (typically 2pm-10pm)
- **split**: Non-contiguous shift with a break in the middle
{% enddocs %}

{% docs supplier_risk_levels %}
Supplier risk is assessed on a composite score combining concentration, reliability, and quality:
- **low**: Composite score 0-25 — reliable supplier with diversified spend
- **medium**: Composite score 26-50 — some concerns, monitor closely
- **high**: Composite score 51-75 — significant risk, develop alternatives
- **critical**: Composite score 76-100 — immediate action required
{% enddocs %}
