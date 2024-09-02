package chaincode

import (
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
	"github.com/hyperledger/fabric-chaincode-go/v2/pkg/cid"
)

func (s *SmartContract) GetMyUID(ctx contractapi.TransactionContextInterface) (string, error) {
    mspid, err := cid.GetMSPID(ctx.GetStub())
    if err != nil {
        return "", fmt.Errorf("failed to read MSP from credential: %v", err)
    }

    uid, ok, err := cid.GetAttributeValue(ctx.GetStub(), "uid")
    if err != nil {
        return "", fmt.Errorf("failed to read attribute from credential: %v", err)
    } else if !ok {
        uid, err = cid.GetID(ctx.GetStub())
        if err != nil {
            return "", fmt.Errorf("failed to read UID from credential: %v", err)
        }

        uid = "$" + uid
    }

    return mspid + "##" + uid, nil
}

