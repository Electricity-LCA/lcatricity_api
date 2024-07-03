-- Table: public.ElectricityGeneration

-- DROP TABLE IF EXISTS public."ElectricityGenerationNew";

CREATE TABLE IF NOT EXISTS public."ElectricityGenerationNew"
(
    "Id" SERIAL NOT NULL  PRIMARY KEY,
    "RegionId" integer,
    "DateStamp" timestamp with time zone,
	"GenerationTypeId" integer,
    "AggregatedGeneration" double precision,
    CONSTRAINT "UX_ElectricityGenerationNew" UNIQUE ("RegionId", "DateStamp"),
    CONSTRAINT "ElectricityGenerationNew_RegionId_fkey" FOREIGN KEY ("RegionId")
        REFERENCES public."Regions" ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
	CONSTRAINT "ElectricityGenerationNew_GenerationType_fkey" FOREIGN KEY ("GenerationTypeId")
        REFERENCES public."ElectricityGenerationTypes" ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."ElectricityGenerationNew"
    OWNER to postgres;