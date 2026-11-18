"""M-Pesa statements and supplier invoices -> validated rows via Gemini (phase 5.1).

TODO:
- extract text with pypdf; send it to Gemini asking for JSON matching schemas.MpesaTransaction / InvoiceLine
- validate with Pydantic; retry once on ValidationError, then dead-letter the document
- hash phone numbers (hashlib.sha256) before they are stored anywhere
- tests use saved sample texts and a fake client: no real API calls in the test suite
"""
