package sync

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum-optimism/optimistic-specs/opnode/eth"
	"github.com/ethereum-optimism/optimistic-specs/opnode/rollup"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

type l2Block struct {
	Self   eth.BlockID
	FromL1 eth.BlockID
}

type mockSyncReference struct {
	L2 []l2Block
	L1 []eth.BlockID
}

func (m *mockSyncReference) RefByL1Num(ctx context.Context, l1Num uint64) (self eth.BlockID, parent eth.BlockID, err error) {
	if l1Num >= uint64(len(m.L1)) {
		err = ethereum.NotFound
		return
	}
	self = m.L1[l1Num]
	if l1Num > 0 {
		parent = m.L1[l1Num-1]
	}
	return
}

func (m *mockSyncReference) L1HeadRef(ctx context.Context) (self eth.BlockID, parent eth.BlockID, err error) {
	l := len(m.L1)
	if l == 0 {
		err = ethereum.NotFound
		return
	}
	self = m.L1[l-1]
	if l-1 > 0 {
		parent = m.L1[l-1-1]
	}
	return
}

func (m *mockSyncReference) RefByL2Num(ctx context.Context, l2Num *big.Int, genesis *rollup.Genesis) (refL1 eth.BlockID, refL2 eth.BlockID, parentL2 common.Hash, err error) {
	if len(m.L2) == 0 {
		panic("bad test, no l2 chain")
	}
	i := uint64(len(m.L2) - 1)
	if l2Num != nil {
		i = l2Num.Uint64()
	}
	head := m.L2[i]
	refL1 = head.FromL1
	refL2 = head.Self
	if i > 0 {
		parentL2 = m.L2[i-1].Self.Hash
	}
	return
}

func (m *mockSyncReference) RefByL2Hash(ctx context.Context, l2Hash common.Hash, genesis *rollup.Genesis) (refL1 eth.BlockID, refL2 eth.BlockID, parentL2 common.Hash, err error) {
	for i, bl := range m.L2 {
		if bl.Self.Hash == l2Hash {
			return m.RefByL2Num(ctx, big.NewInt(int64(i)), genesis)
		}
	}
	err = ethereum.NotFound
	return
}

var _ SyncReference = (*mockSyncReference)(nil)

func mockID(id rune, num uint64) eth.BlockID {
	var h common.Hash
	copy(h[:], string(id))
	return eth.BlockID{Hash: h, Number: uint64(num)}
}

func chainL1(offset uint64, ids string) (out []eth.BlockID) {
	for i, id := range ids {
		out = append(out, mockID(id, offset+uint64(i)))
	}
	return
}

func chainL2(l1 []eth.BlockID, ids string) (out []l2Block) {
	for i, id := range ids {
		out = append(out, l2Block{
			Self:   mockID(id, uint64(i)),
			FromL1: l1[i],
		})
	}
	return
}

type syncStartTestCase struct {
	Name string

	OffsetL2 uint64
	EngineL1 string // L1 Chain prior to a re-org or other change
	EngineL2 string // L2 Chain that follows from L1Chain
	ActualL1 string // L1 Chain after a change may have occurred

	GenesisL1 rune
	GenesisL2 rune

	ExpectedNextRefsL1 string // The L1 extension to follow (i.e. L1 after the L1 parent in the new L2 Head)
	ExpectedRefL2      rune   // The new L2 tip after a L1 change that may have occured

	ExpectedErr error
}

func refToRune(r eth.BlockID) rune {
	return rune(r.Hash.Bytes()[0])
}

func (c *syncStartTestCase) Run(t *testing.T) {
	engL1 := chainL1(c.OffsetL2, c.EngineL1)
	engL2 := chainL2(engL1, c.EngineL2)
	actL1 := chainL1(0, c.ActualL1)

	msr := &mockSyncReference{
		L2: engL2,
		L1: actL1,
	}

	genesis := &rollup.Genesis{
		L1: mockID(c.GenesisL1, c.OffsetL2),
		L2: mockID(c.GenesisL2, 0),
	}

	fmt.Println(engL1)
	fmt.Println(actL1)
	fmt.Println(engL2)

	nextRefL1s, refL2, err := V3FindSyncStart(context.Background(), SyncSourceV2{msr}, genesis)

	if c.ExpectedErr != nil {
		assert.Error(t, err, "Expecting an error in this test case")
		assert.ErrorIs(t, err, c.ExpectedErr)
	} else {
		expectedRefL2 := refToRune(refL2)
		var expectedRefsL1 []rune
		for _, ref := range nextRefL1s {
			expectedRefsL1 = append(expectedRefsL1, refToRune(ref))
		}

		assert.NoError(t, err)
		assert.Equal(t, c.ExpectedNextRefsL1, string(expectedRefsL1), "Next L1 refs not equal")
		assert.Equal(t, expectedRefL2, c.ExpectedRefL2, "Next L2 Head not equal")
	}
}

func TestFindSyncStart(t *testing.T) {
	testCases := []syncStartTestCase{
		{
			Name:               "happy extend",
			OffsetL2:           0,
			EngineL1:           "ab",
			EngineL2:           "AB",
			ActualL1:           "abc",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "c",
			ExpectedRefL2:      'B',
			ExpectedErr:        nil,
		},
		{
			Name:               "extend one at a time",
			OffsetL2:           0,
			EngineL1:           "ab",
			EngineL2:           "AB",
			ActualL1:           "abcdef",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "cdef",
			ExpectedRefL2:      'B',
			ExpectedErr:        nil,
		},
		{
			Name:               "already synced",
			OffsetL2:           0,
			EngineL1:           "abcde",
			EngineL2:           "ABCDE",
			ActualL1:           "abcde",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      'E',
			ExpectedErr:        nil,
		},
		{
			Name:               "genesis",
			OffsetL2:           0,
			EngineL1:           "a",
			EngineL2:           "A",
			ActualL1:           "a",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      'A',
			ExpectedErr:        nil,
		},
		{
			Name:               "reorg two steps back",
			OffsetL2:           0,
			EngineL1:           "abc",
			EngineL2:           "ABC",
			ActualL1:           "axy",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "xy",
			ExpectedRefL2:      'A',
			ExpectedErr:        nil,
		},
		{
			Name:               "Orphan block",
			OffsetL2:           0,
			EngineL1:           "abcd",
			EngineL2:           "ABCD",
			ActualL1:           "abcx",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "x",
			ExpectedRefL2:      'C',
			ExpectedErr:        nil,
		},
		{
			Name:               "L2 chain ahead",
			OffsetL2:           0,
			EngineL1:           "abcdef",
			EngineL2:           "ABCDEF",
			ActualL1:           "abc",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      'C',
			ExpectedErr:        nil,
		},
		{
			Name:               "L2 chain ahead reorg",
			OffsetL2:           0,
			EngineL1:           "abcdef",
			EngineL2:           "ABCDEF",
			ActualL1:           "abcx",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "x",
			ExpectedRefL2:      'C',
			ExpectedErr:        nil,
		},
		{
			Name:               "unexpected L1 chain",
			OffsetL2:           0,
			EngineL1:           "abcdef",
			EngineL2:           "ABCDEF",
			ActualL1:           "xyz",
			GenesisL1:          'a',
			GenesisL2:          'A',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      0,
			ExpectedErr:        WrongChainErr,
		},
		{
			Name:               "unexpected L2 chain",
			OffsetL2:           0,
			EngineL1:           "abcdef",
			EngineL2:           "ABCDEF",
			ActualL1:           "xyz",
			GenesisL1:          'a',
			GenesisL2:          'X',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      0,
			ExpectedErr:        WrongChainErr,
		},
		{
			Name:               "offset L2 genesis extend",
			OffsetL2:           3,
			EngineL1:           "def",
			EngineL2:           "DEF",
			ActualL1:           "abcdefg",
			GenesisL1:          'd',
			GenesisL2:          'D',
			ExpectedNextRefsL1: "g",
			ExpectedRefL2:      'F',
			ExpectedErr:        nil,
		},
		{
			Name:               "offset L2 genesis reorg",
			OffsetL2:           3,
			EngineL1:           "defgh",
			EngineL2:           "DEFGH",
			ActualL1:           "abcdx",
			GenesisL1:          'd',
			GenesisL2:          'D',
			ExpectedNextRefsL1: "x",
			ExpectedRefL2:      'D',
			ExpectedErr:        nil,
		},
		{
			Name:               "reorg past offset genesis",
			OffsetL2:           3,
			EngineL1:           "abcdefgh",
			EngineL2:           "ABCDEFGH",
			ActualL1:           "abx",
			GenesisL1:          'd',
			GenesisL2:          'D',
			ExpectedNextRefsL1: "",
			ExpectedRefL2:      0,
			ExpectedErr:        WrongChainErr,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, testCase.Run)
	}
}
