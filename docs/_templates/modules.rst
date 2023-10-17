:mod:`{{fullname}}`
{{ underline }}====================

.. rubric:: Description

.. automodule:: {{ fullname }}
   :no-members:
   :no-inherited-members:

.. currentmodule:: {{ fullname }}

{% if classes %}
.. rubric:: Classes

.. autosummary::
    :toctree: .
    :template: class.rst
    {% for class in classes %}
    {{ class }}
    {% endfor %}

{% endif %}

{% if functions %}
.. rubric:: Functions

.. autosummary::
    :toctree: .
    :template: function.rst
    {% for function in functions %}
    {{ function }}
    {% endfor %}

{% endif %}
