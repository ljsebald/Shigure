/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/google/uuid"
)

// Initializer for new blockchains.
func (s *SmartContract) initusers(ctx contractapi.TransactionContextInterface) error {
    // Add the calling user as an admin to the system.
    myuid, err := s.GetMyUID(ctx)
    if err != nil {
        return err
    }

    _, err = s.adduser_int(ctx, myuid, "", 0xffffffff)
    if err != nil {
        return err
    }

    return nil
}

func (s *SmartContract) GetMyUser(ctx contractapi.TransactionContextInterface) (*User, error) {
    myuid, err := s.GetMyUID(ctx)
    if err != nil {
        return nil, err
    }

    return s.GetUserByUID(ctx, myuid)
}

func (s *SmartContract) GetUserByUID(ctx contractapi.TransactionContextInterface,
                                     uid string) (*User, error) {
    // TODO: Use explicit index
    query := fmt.Sprintf(`{"selector":{"type":"User","uid":"%s"}}`, uid)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var user User
        err = json.Unmarshal(queryResponse.Value, &user)
        if err != nil {
            return nil, err
        }

        return &user, nil
    }

    return nil, fmt.Errorf("failed to look up user with uid: %v", uid)
}

func (s *SmartContract) GetUserByID(ctx contractapi.TransactionContextInterface,
                                    id string) (*User, error) {
    sid, _ := ctx.GetStub().CreateCompositeKey("User", []string{id})
    usrJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return nil, err
    } else if usrJSON == nil {
        return nil, fmt.Errorf("unknown user")
    }

    var user User
    err = json.Unmarshal(usrJSON, &user)
    if err != nil {
        return nil, err
    }

    return &user, nil
}

func (s *SmartContract) AddUser(ctx contractapi.TransactionContextInterface,
                                uid string, sysperms uint32) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    if (myuser.SysPerms & User_SysPerms_AddUsers) == 0 {
        return "", fmt.Errorf("permission denied")
    }

    return s.adduser_int(ctx, uid, "", sysperms)
}

func (s *SmartContract) adduser_int(ctx contractapi.TransactionContextInterface,
                                    uid string, parent string,
                                    sysperms uint32) (string, error) {
    tmp, _ := s.GetUserByUID(ctx, uid)
    if tmp != nil {
        return "", fmt.Errorf("user already exists")
    }

    newuser := User {
        Type:       "User",
        ID:         uuid.NewString(),
        UID:        uid,
        Parent:     parent,
        SysPerms:   sysperms,
        SubUsers:   make([]SubUser, 0),
    }

    usrJSON, err := json.Marshal(newuser)
    if err != nil {
        return "", err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("User", []string{newuser.ID})
    err = ctx.GetStub().PutState(stateid, usrJSON)
    if err != nil {
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return newuser.ID, nil
}

func (s *SmartContract) GetAllUsers(ctx contractapi.TransactionContextInterface) ([]*User, error) {
    resultsIterator, err := ctx.GetStub().GetStateByPartialCompositeKey("User", []string{})
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var users []*User
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var user User
        err = json.Unmarshal(queryResponse.Value, &user)
        if err != nil {
            return nil, err
        }

        users = append(users, &user)
    }

    return users, nil
}

func (s *SmartContract) AddSubUser(ctx contractapi.TransactionContextInterface,
                                   uid string, perms map[string]uint32,
                                   sysperms uint32) (string, error) {
    // Sub-users can't add new regular users.
    if (sysperms & User_SysPerms_AddUsers) != 0 {
        return "", fmt.Errorf("invalid system permissions")
    }

    // Make sure we're allowed to do this...
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    if (myuser.SysPerms & User_SysPerms_AddSubUsers) == 0 {
        return "", fmt.Errorf("permission denied")
    }

    // Add the user account
    newid, err := s.adduser_int(ctx, uid, myuser.ID, sysperms)
    if err != nil {
        return "", err
    }

    // Add the user to our list of sub-users and update our entry
    su := SubUser {
        ID:     newid,
        UID:    uid,
        Perms:  perms,
    }

    myuser.SubUsers = append(myuser.SubUsers, su)
    stateid, _ := ctx.GetStub().CreateCompositeKey("User", []string{myuser.ID})

    usrJSON, err := json.Marshal(myuser)
    if err != nil {
        return "", err
    }

    err = ctx.GetStub().PutState(stateid, usrJSON)
    if err != nil {
        // uh oh...
        stateid, _ := ctx.GetStub().CreateCompositeKey("User", []string{newid})
        ctx.GetStub().DelState(stateid)
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return newid, nil
}

func (s *SmartContract) SetSubUserPermission(ctx contractapi.TransactionContextInterface,
                                             uid string, bucket string,
                                             perms uint32) (bool, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look for the specified subuser...
    for _, ent := range user.SubUsers {
        if ent.UID == uid {
            ent.Perms[bucket] = perms

            // Update our state in the db
            usrJSON, err := json.Marshal(user)
            if err != nil {
                return false, err
            }

            id, _ := ctx.GetStub().CreateCompositeKey("User", []string{user.ID})
            err = ctx.GetStub().PutState(id, usrJSON)
            if err != nil {
                return false, fmt.Errorf("failed to put to world state. %v", err)
            }

            return true, nil
        }
    }

    return false, fmt.Errorf("unknown subuser")
}

func (s *SmartContract) RevokeSubUserPermission(ctx contractapi.TransactionContextInterface,
                                                uid string,
                                                bucket string) (bool, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look for the specified subuser...
    for _, ent := range user.SubUsers {
        if ent.ID == uid {
            delete(ent.Perms, bucket)

            // Update our state in the db
            usrJSON, err := json.Marshal(user)
            if err != nil {
                return false, err
            }

            id, _ := ctx.GetStub().CreateCompositeKey("User", []string{user.ID})
            err = ctx.GetStub().PutState(id, usrJSON)
            if err != nil {
                return false, fmt.Errorf("failed to put to world state. %v", err)
            }

            return true, nil
        }
    }

    return false, fmt.Errorf("unknown subuser")
}

func (s *SmartContract) IsUserMyDescendent(ctx contractapi.TransactionContextInterface,
                                           uid string) (bool, error) {
    me, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if me == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Start from the specified user and go toward the root of the tree.
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    for {
        if user.Parent == "" {
            return false, nil
        }

        if user.Parent == me.ID {
            return true, nil
        }

        user, err := s.GetUserByID(ctx, user.Parent)
        if err != nil {
            return false, err
        } else if user == nil {
            return false, fmt.Errorf("unknown user")
        }
    }
}

func (s *SmartContract) GatherMyInheritedPerms(ctx contractapi.TransactionContextInterface,
                                               bucket string) (map[string]uint32, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.gatheruperms(ctx, user, bucket)
}

func (s *SmartContract) GatherUserInheritedPerms(ctx contractapi.TransactionContextInterface,
                                                 uid string,
                                                 bucket string) (map[string]uint32, error) {
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.gatheruperms(ctx, user, bucket)
}

func (s *SmartContract) gatheruperms(ctx contractapi.TransactionContextInterface,
                                     user *User, bucket string) (map[string]uint32, error) {
    rv := map[string]uint32{}
    var parent *User = nil
    var lastperms uint32 = 0x000000ff

    // The user has full permissions for anything they have direct access to.
    rv[user.ID] = lastperms

    // Iterate up the tree of parents until we either run out of permissions or
    // get all the way to the root
    for u := user; u.Parent != "" && lastperms != 0; u = parent {
        // Grab the parent.
        parent, err := s.GetUserByID(ctx, u.Parent)
        if err != nil {
            return nil, err
        } else if parent == nil {
            return nil, fmt.Errorf("unknown user in hierarchy")
        }

        // Find our entry in the subusers
        for _, ent := range parent.SubUsers {
            if ent.ID == u.ID {
                // Look for the bucket in question
                perms, ok := ent.Perms[bucket]
                if !ok || perms == 0 {
                    // If we didn't match the bucket, see if we have a wildcard
                    // match. Specific matches always override wildcard ones.
                    perms, ok = ent.Perms["*"]

                    if !ok || perms == 0 {
                        // We don't have anything further to do up this path
                        // since we don't have either a specific or wildcard
                        // match
                        return rv, nil
                    }
                }

                // Apply the permissions we have here to what we've gotten
                // so far... Record it if we've got something left.
                lastperms &= perms
                if lastperms != 0 {
                    rv[parent.ID] = lastperms
                }
            }
        }
    }

    // We've reached the root (or no further permissions) if we get here. Return
    // what we found.
    return rv, nil
}

