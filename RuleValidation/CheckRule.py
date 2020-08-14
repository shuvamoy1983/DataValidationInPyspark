
from RuleValidation import AllRuleValidation
class RuleCheck:

    def ruleMap(inComingRule, attribute, metadaDF, data,n):
        if inComingRule == 'DQ6':
            AllRuleValidation.AttributeValidate.chkNull(attribute, data, metadaDF, inComingRule,n)
        elif (inComingRule == 'DQ1'):
            pass

    def Multi_rule_validate(rule, attribute, metadaDF, data,n):

        for i in rule:
            RuleCheck.ruleMap(i, attribute, metadaDF, data,n)

    def Single_rule_validate(rule, attribute, metadaDF, data,n):
        RuleCheck.ruleMap(rule, attribute, metadaDF, data,n)


