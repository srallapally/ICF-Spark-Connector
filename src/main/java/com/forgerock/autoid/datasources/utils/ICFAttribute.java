package com.forgerock.autoid.datasources.utils;

import org.identityconnectors.framework.common.objects.AttributeInfo;

import java.io.Serializable;
import java.util.Set;

public class ICFAttribute implements Serializable {
    private static final long serialVersionUID = 7302778500155477919L;
    private String attributeName;
    private Class<?> attributeType;
    private Set<AttributeInfo.Flags> flags;
    private boolean required;

    public ICFAttribute(){}

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Class<?> getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(Class<?> attributeType) {
        this.attributeType = attributeType;
    }

    public Set<AttributeInfo.Flags> getFlags() {
        return flags;
    }

    public void setFlags(Set<AttributeInfo.Flags> flags) {
        this.flags = flags;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
