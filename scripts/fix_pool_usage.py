#!/usr/bin/env python3
"""
Script to replace self.pool() calls with appropriate read_pool() or write_pool() calls.
Reads: SELECT, fetch_optional, fetch_all, fetch_one
Writes: INSERT, UPDATE, DELETE, execute (when not after SELECT)
"""

import re
import sys

def determine_pool_type(lines, line_idx):
    """
    Determine if the pool() call should be read_pool() or write_pool().
    Looks at surrounding context to decide.
    """
    # Get a few lines of context before the pool() call
    start = max(0, line_idx - 10)
    context = ''.join(lines[start:line_idx+1])
    
    # Check for write operations (INSERT, UPDATE, DELETE, or BEGIN IMMEDIATE)
    write_patterns = [
        r'\bINSERT\b',
        r'\bUPDATE\b', 
        r'\bDELETE\b',
        r'\bBEGIN\s+IMMEDIATE\b',
        r'INSERT OR IGNORE',
        r'INSERT OR REPLACE',
    ]
    
    for pattern in write_patterns:
        if re.search(pattern, context, re.IGNORECASE):
            return 'write_pool'
    
    # Check for read operations (SELECT, PRAGMA that reads)
    read_patterns = [
        r'\bSELECT\b',
        r'\bPRAGMA\s+(synchronous|journal_mode|foreign_keys|integrity_check|user_version)\b',
    ]
    
    for pattern in read_patterns:
        if re.search(pattern, context, re.IGNORECASE):
            return 'read_pool'
    
    # Special case: pool assigned to variable (often for transactions)
    if 'let pool = self.pool()' in lines[line_idx] or 'let p = self.pool()' in lines[line_idx]:
        # Look ahead to see how it's used
        ahead_start = line_idx
        ahead_end = min(len(lines), line_idx + 20)
        ahead_context = ''.join(lines[ahead_start:ahead_end])
        for pattern in write_patterns:
            if re.search(pattern, ahead_context, re.IGNORECASE):
                return 'write_pool'
        return 'read_pool'
    
    # Default to read_pool (safer for unknown cases)
    return 'read_pool'

def process_file(filepath):
    """Process a single file, replacing self.pool() calls."""
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    modified = False
    new_lines = []
    
    for i, line in enumerate(lines):
        if 'self.pool()' in line:
            pool_type = determine_pool_type(lines, i)
            new_line = line.replace('self.pool()', f'self.{pool_type}()')
            new_lines.append(new_line)
            modified = True
            print(f"Line {i+1}: {line.strip()} -> {pool_type}")
        else:
            new_lines.append(line)
    
    if modified:
        with open(filepath, 'w') as f:
            f.writelines(new_lines)
        print(f"✓ Modified {filepath}")
        return True
    else:
        print(f"- No changes needed for {filepath}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: fix_pool_usage.py <file1> <file2> ...")
        sys.exit(1)
    
    modified_count = 0
    for filepath in sys.argv[1:]:
        if process_file(filepath):
            modified_count += 1
    
    print(f"\nProcessed {len(sys.argv)-1} files, modified {modified_count} files.")
