"""
Striim pipeline SRE agent
=========================

A control-plane agent for Striim data pipelines. It sits beside the
pipelines, never in the data path. It reads Striim's control plane, works
out what is wrong when something breaks, fixes what it safely can, and
escalates what it cannot - with an explanation a tired human can act on.

Striim already detects failures and already sends alerts. What is manual is
everything after the alert: working out WHY, deciding the fix, applying it,
and confirming it worked. The alert says what broke. It never says why, and
one symptom has many causes with different fixes. That gap is the product.

The central rule, which everything here is arranged around:

    The model recommends. The code decides.

    python -m agent doctor
    python -m agent monitor sarah_moolai.Trial5
"""

__version__ = "1.0.0"
