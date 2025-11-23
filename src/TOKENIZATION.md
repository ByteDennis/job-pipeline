
**Task**: Implement tokenization validation functionality as a new step (potentially step 6 or separate script)

**User's explicit request**: "the last task (which is a sepreate task to step 1->5) is to compare xxx in pcds versus its tokenized value yyy in aws on every provided pairs of tables (pcds->aws, with provided to_hash columns), generate a _hash value (borrow step 5 normalization and hashing, but only on provided to_hash columns) then use it to join tables, obtain xxx -> yyy for each paired _hash value, then compare this mapping against globally maintained mapping to determine if tokenization function is consistent and correct"

**Approach**:
1. Create new file: `src/06_tokenization_validation.py` (or similar)
2. Reuse normalization and hashing logic from step 5 (utils.normalize_oracle_column, utils.build_oracle_hash_expr, etc.)
3. Generate queries to:
   - Hash identifying columns in PCDS tables
   - Hash corresponding columns in AWS tables
   - Join on hash to establish xxx → yyy mapping
4. Compare discovered mappings against globally maintained dictionary
5. Flag inconsistencies and unexpected mappings
6. Follow the same simplified argparse pattern (single run_name argument)
7. Include in the resumable workflow with state management

This task requires clarification on:
- Format of the globally maintained dictionary (CSV, JSON, database table?)
- Which columns to hash (is this in the crosswalk document or a separate config?)
- What constitutes an "inconsistency" vs expected variation?.
Please continue the conversation from where we left it off without asking the user any further questions. Continue with the last task that you were asked to work on.