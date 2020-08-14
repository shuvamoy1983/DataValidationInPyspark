
from RuleValidation import AllRuleValidation
class RuleCheck:

    def ruleMap(inComingRule, attribute, metadaDF, data):
        if inComingRule == 'DQ6':
            AllRuleValidation.AttributeValidate.chkNull(attribute, data, metadaDF, inComingRule)
        elif (inComingRule == 'DQ1'):
            pass

    def Multi_rule_validate(rule, attribute, metadaDF, data):
        for i in rule:
            RuleCheck.ruleMap(i, attribute, metadaDF, data)

    def Single_rule_validate(rule, attribute, metadaDF, data):
        print('i', rule, attribute)
        RuleCheck.ruleMap(rule, attribute, metadaDF, data,)


