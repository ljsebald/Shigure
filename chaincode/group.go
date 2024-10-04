/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"
    "slices"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/google/uuid"
)

// Initializer for new blockchains.
func (s *SmartContract) initgroups(ctx contractapi.TransactionContextInterface) error {
    // Create a "none" group
    grp := Group {
        Type:       "Group",
        ID:         "ffffffff-ffff-ffff-ffff-ffffffffffff",
        Name:       "none",
        Owner:      "",
        Parent:     "",
        Users:      make([]string, 0),
        SubGroups:  make([]SubGroup, 0),
    }

    grpJSON, err := json.Marshal(grp)
    if err != nil {
        return err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("Group", []string{grp.ID})
    err = ctx.GetStub().PutState(stateid, grpJSON)
    if err != nil {
        return fmt.Errorf("failed to put to world state. %v", err)
    }

    return nil
}

// Search for a group by name
func (s *SmartContract) GetGroupByName(ctx contractapi.TransactionContextInterface,
                                       name string) (*Group, error) {
    // TODO: Use explicit index
    query := fmt.Sprintf(`{"selector":{"type":"Group","name":"%s"}}`, name)
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

        var grp Group
        err = json.Unmarshal(queryResponse.Value, &grp)
        if err != nil {
            return nil, err
        }

        return &grp, nil
    }

    return nil, fmt.Errorf("failed to look up group with name: %v", name)
}

// Search for a group by it's UUID
func (s *SmartContract) GetGroupByID(ctx contractapi.TransactionContextInterface,
                                     id string) (*Group, error) {
    sid, _ := ctx.GetStub().CreateCompositeKey("Group", []string{id})
    grpJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return nil, err
    } else if grpJSON == nil {
        return nil, fmt.Errorf("unknown group")
    }

    var grp Group
    err = json.Unmarshal(grpJSON, &grp)
    if err != nil {
        return nil, err
    }

    return &grp, nil
}

// Add a new group to the system, owned by the caller
func (s *SmartContract) AddGroup(ctx contractapi.TransactionContextInterface,
                                 name string, addme bool) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    if (myuser.SysPerms & 0x04) == 0 {
        return "", fmt.Errorf("permission denied")
    }

    return s.addgroup_int(ctx, name, myuser.ID, "", addme)
}

func (s *SmartContract) addgroup_int(ctx contractapi.TransactionContextInterface,
                                     name string, owner string,
                                     parent string,
                                     addowner bool) (string, error) {
    tmp, _ := s.GetGroupByName(ctx, name)
    if tmp != nil {
        return "", fmt.Errorf("group already exists")
    }

    grp := Group {
        Type:       "Group",
        ID:         uuid.NewString(),
        Name:       name,
        Owner:      owner,
        Parent:     parent,
        SubGroups:  make([]SubGroup, 0),
    }

    if addowner {
        grp.Users = make([]string, 1)
        grp.Users[0] = owner
    } else {
        grp.Users = make([]string, 0)
    }

    grpJSON, err := json.Marshal(grp)
    if err != nil {
        return "", err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("Group", []string{grp.ID})
    err = ctx.GetStub().PutState(stateid, grpJSON)
    if err != nil {
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return grp.ID, nil
}

// Retrieve a list of all groups in the system
func (s *SmartContract) GetAllGroups(ctx contractapi.TransactionContextInterface) ([]*Group, error) {
    resultsIterator, err := ctx.GetStub().GetStateByPartialCompositeKey("Group", []string{})
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var groups []*Group
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var grp Group
        err = json.Unmarshal(queryResponse.Value, &grp)
        if err != nil {
            return nil, err
        }

        groups = append(groups, &grp)
    }

    return groups, nil
}

// Add a sub-group of the specified group, owned by the caller
func (s *SmartContract) AddSubGroup(ctx contractapi.TransactionContextInterface,
                                    pname string, name string,
                                    perms map[string]uint32, addme bool) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    if (myuser.SysPerms & 0x04) == 0 {
        return "", fmt.Errorf("permission denied")
    }

    // Look up the parent group to see if the calling user owns it
    pgrp, err := s.GetGroupByName(ctx, pname)
    if err != nil || pgrp == nil {
        return "", fmt.Errorf("group not found")
    }

    if pgrp.Owner != myuser.ID {
        return "", fmt.Errorf("permission denied")
    }

    // Add the group
    newid, err := s.addgroup_int(ctx, name, myuser.ID, pgrp.ID, addme)
    if err != nil {
        return "", err
    }

    // Add the group to the list of sub-groups and update our entry
    sg := SubGroup {
        ID:     newid,
        Name:   name,
        Perms:  perms,
    }

    pgrp.SubGroups = append(pgrp.SubGroups, sg)
    stateid, _ := ctx.GetStub().CreateCompositeKey("Group", []string{pgrp.ID})

    grpJSON, err := json.Marshal(pgrp)
    if err != nil {
        return "", err
    }

    err = ctx.GetStub().PutState(stateid, grpJSON)
    if err != nil {
        // uh oh...
        stateid, _ := ctx.GetStub().CreateCompositeKey("Group", []string{newid})
        ctx.GetStub().DelState(stateid)
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return newid, nil
}

// Add bucket permissions to be inherited from the parent group by a specified
// sub-group
func (s *SmartContract) SetSubGroupPermission(ctx contractapi.TransactionContextInterface,
                                              pname string, sname string,
                                              bucket string,
                                              perms uint32) (bool, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look up the parent group and make sure we own it
    pgrp, err := s.GetGroupByName(ctx, pname)
    if err != nil || pgrp == nil {
        return false, fmt.Errorf("group not found")
    }

    if pgrp.Owner != user.ID {
        return false, fmt.Errorf("permission denied")
    }

    // Look for the specified subgroup...
    for _, ent := range pgrp.SubGroups {
        if ent.Name == sname {
            ent.Perms[bucket] = perms

            // Update our state in the db
            grpJSON, err := json.Marshal(pgrp)
            if err != nil {
                return false, err
            }

            id, _ := ctx.GetStub().CreateCompositeKey("Group", []string{pgrp.ID})
            err = ctx.GetStub().PutState(id, grpJSON)
            if err != nil {
                return false, fmt.Errorf("failed to put to world state. %v", err)
            }

            return true, nil
        }
    }

    return false, fmt.Errorf("unknown subgroup")
}

// Revoke the inherited permissions for the specified bucket from a sub-group
func (s *SmartContract) RevokeSubGroupPermission(ctx contractapi.TransactionContextInterface,
                                                 pname string, sname string,
                                                 bucket string) (bool, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look up the parent group and make sure we own it
    pgrp, err := s.GetGroupByName(ctx, pname)
    if err != nil || pgrp == nil {
        return false, fmt.Errorf("group not found")
    }

    if pgrp.Owner != user.ID {
        return false, fmt.Errorf("permission denied")
    }

    // Look for the specified subgroup...
    for _, ent := range pgrp.SubGroups {
        if ent.Name == sname {
            delete(ent.Perms, bucket)

            // Update our state in the db
            grpJSON, err := json.Marshal(pgrp)
            if err != nil {
                return false, err
            }

            id, _ := ctx.GetStub().CreateCompositeKey("Group", []string{pgrp.ID})
            err = ctx.GetStub().PutState(id, grpJSON)
            if err != nil {
                return false, fmt.Errorf("failed to put to world state. %v", err)
            }

            return true, nil
        }
    }

    return false, fmt.Errorf("unknown subgroup")
}

// Get all groups that the caller is a direct member of
func (s *SmartContract) GetMyMemberGroups(ctx contractapi.TransactionContextInterface) ([]*Group, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.getusergroups(ctx, user.ID)
}

// Get all groups that the specified user is a direct member of
func (s *SmartContract) GetMemberGroupsForUID(ctx contractapi.TransactionContextInterface,
                                              uid string) ([]*Group, error) {
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.getusergroups(ctx, user.ID)
}

func (s *SmartContract) getusergroups(ctx contractapi.TransactionContextInterface,
                                      id string) ([]*Group, error) {
    query := fmt.Sprintf(`{"selector":{"type":"Group","users":{"$elemMatch":{"$eq":"%s"}}`, id)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var groups []*Group
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var grp Group
        err = json.Unmarshal(queryResponse.Value, &grp)
        if err != nil {
            return nil, err
        }

        groups = append(groups, &grp)
    }

    return groups, nil
}

// Get all groups owned by the calling user
func (s *SmartContract) GetMyOwnedGroups(ctx contractapi.TransactionContextInterface) ([]*Group, error) {
    user, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.getuserownedgroups(ctx, user.ID)
}

// Get all groups owned by the specified user
func (s *SmartContract) GetOwnedGroupsForUID(ctx contractapi.TransactionContextInterface,
                                             uid string) ([]*Group, error) {
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return nil, err
    } else if user == nil {
        return nil, fmt.Errorf("unknown user")
    }

    return s.getuserownedgroups(ctx, user.ID)
}

func (s *SmartContract) getuserownedgroups(ctx contractapi.TransactionContextInterface,
                                           id string) ([]*Group, error) {
    query := fmt.Sprintf(`{"selector":{"type":"Group","owner":"%s"}}`, id)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var groups []*Group
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var grp Group
        err = json.Unmarshal(queryResponse.Value, &grp)
        if err != nil {
            return nil, err
        }

        groups = append(groups, &grp)
    }

    return groups, nil
}

// Add the specified user to a group (by the group's name)
func (s *SmartContract) AddUserToGroup(ctx contractapi.TransactionContextInterface,
                                       name string, uid string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if myuser == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look up the group and make sure we own it
    grp, err := s.GetGroupByName(ctx, name)
    if err != nil || grp == nil {
        return false, fmt.Errorf("group not found")
    }

    if grp.Owner != myuser.ID {
        return false, fmt.Errorf("permission denied")
    }

    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Make sure the user isn't already a member.
    if slices.Contains(grp.Users, user.ID) {
        return false, fmt.Errorf("already a member")
    }

    // Update our state in the db
    grp.Users = append(grp.Users, user.ID)

    grpJSON, err := json.Marshal(grp)
    if err != nil {
        return false, err
    }

    id, _ := ctx.GetStub().CreateCompositeKey("Group", []string{grp.ID})
    err = ctx.GetStub().PutState(id, grpJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

// Remove the specified user from a group (by the group's name)
func (s *SmartContract) RemoveUserFromGroup(ctx contractapi.TransactionContextInterface,
                                            name string,
                                            uid string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    } else if myuser == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Look up the group and make sure we own it
    grp, err := s.GetGroupByName(ctx, name)
    if err != nil || grp == nil {
        return false, fmt.Errorf("group not found")
    }

    if grp.Owner != myuser.ID {
        return false, fmt.Errorf("permission denied")
    }

    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return false, err
    } else if user == nil {
        return false, fmt.Errorf("unknown user")
    }

    // Make sure the user is a member.
    i := slices.Index(grp.Users, user.ID)
    if i == -1 {
        return false, fmt.Errorf("not a member")
    }

    // Update our state in the db
    grp.Users = append(grp.Users[:i], grp.Users[i + 1:]...)
    grpJSON, err := json.Marshal(grp)
    if err != nil {
        return false, err
    }

    id, _ := ctx.GetStub().CreateCompositeKey("Group", []string{grp.ID})
    err = ctx.GetStub().PutState(id, grpJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

// Gather the permissions inherited from ancestor groups on the specified bucket
func (s *SmartContract) GatherGroupInheritedPerms(ctx contractapi.TransactionContextInterface,
                                                  name string,
                                                  bucket string) (map[string]uint32, error) {
    group, err := s.GetGroupByName(ctx, name)
    if err != nil {
        return nil, err
    } else if group == nil {
        return nil, fmt.Errorf("unknown group")
    }

    return s.gathergperms(ctx, group, bucket)
}

func (s *SmartContract) gathergperms(ctx contractapi.TransactionContextInterface,
                                     group *Group, bucket string) (map[string]uint32, error) {
    rv := map[string]uint32{}
    var parent *Group = nil
    var lastperms uint32 = 0x000000ff

    // Iterate up the tree of parents until we either run out of permissions or
    // get all the way to the root
    for g := group; g.Parent != "" && lastperms != 0; g = parent {
        // Grab the parent.
        parent, err := s.GetGroupByID(ctx, g.Parent)
        if err != nil {
            return nil, err
        } else if parent == nil {
            return nil, fmt.Errorf("unknown group in hierarchy")
        }

        // Find our entry in the subgroups
        for _, ent := range parent.SubGroups {
            if ent.ID == g.ID {
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

// Gather all group permissions for a given user and all the groups they are in
func (s *SmartContract) GatherGroupPermsForUser(ctx contractapi.TransactionContextInterface,
                                                uid string,
                                                bucket string) (map[string]uint32, error) {
    // Fetch the user's groups
    groups, err := s.GetMemberGroupsForUID(ctx, uid)
    if err != nil {
        return nil, err
    }


    return s.gatherallgperms(ctx, groups, bucket)
}

func (s *SmartContract) GatherGroupPermsForUserByID(ctx contractapi.TransactionContextInterface,
                                                    id string,
                                                    bucket string) (map[string]uint32, error) {
    // Fetch the user's groups
    groups, err := s.getusergroups(ctx, id)
    if err != nil {
        return nil, err
    }


    return s.gatherallgperms(ctx, groups, bucket)
}

func (s *SmartContract) gatherallgperms(ctx contractapi.TransactionContextInterface,
                                        groups []*Group,
                                        bucket string) (map[string]uint32, error) {
    rv := map[string]uint32{}
    var parent *Group = nil
    var lastperms uint32 = 0x000000ff

    // Run through each group in the array...
    for _, group := range groups {
        // Full permissions are given for any group the user is directly in, so
        // add that in first.
        lastperms = 0x000000ff
        rv[group.ID] = lastperms

        // Iterate up the tree of parents until we either run out of permissions
        // or get all the way to the root
        for g := group; g.Parent != "" && lastperms != 0; g = parent {
            // Grab the parent.
            parent, err := s.GetGroupByID(ctx, g.Parent)
            if err != nil {
                return nil, err
            } else if parent == nil {
                return nil, fmt.Errorf("unknown group in hierarchy")
            }

            // Find our entry in the subgroups
            for _, ent := range parent.SubGroups {
                if ent.ID == g.ID {
                    // Look for the bucket in question
                    perms, ok := ent.Perms[bucket]
                    if !ok || perms == 0 {
                        // If we didn't match the bucket, see if we have a
                        // wildcard match. Specific matches always override
                        //wildcard ones.
                        perms, ok = ent.Perms["*"]

                        if !ok || perms == 0 {
                            // We don't have anything further to do up this path
                            // since we don't have either a specific or wildcard
                            // match
                            break
                        }
                    }

                    // Apply the permissions we have here to what we've gotten
                    // so far... Record it if we've got something left and it is
                    // more permission than we currently have on this bucket.
                    lastperms &= perms
                    if lastperms != 0  && lastperms > rv[parent.ID] {
                        rv[parent.ID] = lastperms
                    }
                }
            }
        }
    }

    // We've finished with every group the user is in, so... we're done.
    return rv, nil
}

