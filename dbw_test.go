package dbw

import (
	"testing"
	"time"

	"github.com/axkit/date"
)

func TestFieldNames(t *testing.T) {

	type UserRow struct {
		ID                       int       `dbw:"noseq"` // id                      INT8                        NOT NULL,
		TypeID                   int       // type_id                 INT2                        NOT NULL,
		StateID                  int       // state_id                INT2                        NOT NULL,
		GenderID                 *int      // gender_id               INT2,
		Nickname                 *string   // nickname                VARCHAR(64),
		EncryptedFullName        *string   // encrypted_full_name     TEXT,
		HashedFullName           *string   // hashed_full_name        TEXT,
		EncryptedLegalName       *string   // encrypted_legal_name    TEXT,
		HashedLegalName          *string   // hashed_legal_name       TEXT,
		EncryptedFirstName       *string   // encrypted_first_name    TEXT,
		HashedFirstName          *string   // hashed_first_name       TEXT,
		EncryptedLastName        *string   // encrypted_last_name     TEXT,
		HashedLastName           *string   // hashed_last_name        TEXT,
		Dob                      date.Date // dob                     DATE,
		EncryptedAadhar          *string   // encrypted_aadhar        TEXT,
		HashedAadhar             *string   // hashed_aadhar           TEXT,
		EncryptedPan             *string   // encrypsted_pan           TEXT,
		HashedPan                *string   // hashed_pan              TEXT,
		IsPep                    *bool     // is_pep                  BOOLEAN,
		DefaultRefcodeID         *int      `dbw:"bms"`
		DefaultRefcodeAssignedAt NullTime
		FaceFingerprint          *string   `dbw:"face"`
		RowVersion               int       // row_version INT8 DEFAULT 0              NOT NULL,
		CreatedBy                int       // created_by          INT8                        NOT NULL,
		CreatedAt                time.Time // created_at          TIMESTAMPTZ DEFAULT NOW()   NOT NULL,
		UpdatedBy                *int      // updated_by          INT8,
		UpdatedAt                NullTime  // updated_at          TIMESTAMPTZ,
		DeletedBy                *int      // deleted_by          INT8,
		DeletedAt                NullTime  // deleted_at          TIMESTAMPTZ,
	}

	s := FieldNames(&UserRow{}, "")
	t.Log(s)
}
