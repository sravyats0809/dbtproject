SELECT
    {{ table_reverse('class', 'code') }} AS code_list,
    {{ class_check('class', 'code', "'cd'") }} AS column_product
FROM 
{{ref("class")}}


