from pydantic import BaseModel, Field


class StockCodeMappingBase(BaseModel):
    standard_code: str = Field(..., description="표준코드 (ISU_CD)")
    short_code: str = Field(..., description="단축코드 (ISU_SRT_CD)")
    stock_name_abbr: str = Field(..., description="한글종목약명 (ISU_ABBRV)")


class StockCodeMappingCreate(StockCodeMappingBase):
    pass


class StockCodeMappingResponse(StockCodeMappingBase):
    class Config:
        from_attributes = True
