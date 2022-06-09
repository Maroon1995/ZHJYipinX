
def FieldToGBK(fields):
    """
    如果字段内容为空，则不强转gbk
    :param fields: 字段内容
    :return:
    """
    if fields != None:
        fields = fields.encode('latin-1').decode('gbk')
    return fields
