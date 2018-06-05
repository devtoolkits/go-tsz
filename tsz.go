// Package tsz implement time-series compression
/*

http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

*/
package tsz

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"sync"
)

// Series is the basic series primitive
// you can concurrently put values, finish the stream, and create iterators
type Series struct {
	sync.Mutex

	// TODO(dgryski): timestamps in the paper are uint64
	T0  uint32
	t   uint32
	val float64

	bw       bstream
	leading  uint8
	trailing uint8
	finished bool

	tDelta uint32
}

// New series
func New(t0 uint32) *Series {
	s := Series{
		T0:      t0,
		leading: ^uint8(0),
	}

	// block header
	s.bw.writeBits(uint64(t0), 32)

	return &s

}

// Bytes value of the series stream
func (s *Series) Bytes() []byte {
	s.Lock()
	defer s.Unlock()
	return s.bw.bytes()
}

func finish(w *bstream) {
	// write an end-of-stream record
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

// Finish the series by writing an end-of-stream record
func (s *Series) Finish() {
	s.Lock()
	if !s.finished {
		finish(&s.bw)
		s.finished = true
	}
	s.Unlock()
}

// Push a timestamp and value to the series
func (s *Series) Push(t uint32, v float64) {
	s.Lock()
	defer s.Unlock()

	if s.t == 0 {
		// first point
		s.t = t
		s.val = v
		s.tDelta = t - s.T0
		s.bw.writeBits(uint64(s.tDelta), 14)
		s.bw.writeBits(math.Float64bits(v), 64)
		return
	}

	tDelta := t - s.t
	dod := int32(tDelta - s.tDelta)

	switch {
	case dod == 0:
		s.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		s.bw.writeBits(0x02, 2) // '10'
		s.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		s.bw.writeBits(0x06, 3) // '110'
		s.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2048:
		s.bw.writeBits(0x0e, 4) // '1110'
		s.bw.writeBits(uint64(dod), 12)
	default:
		s.bw.writeBits(0x0f, 4) // '1111'
		s.bw.writeBits(uint64(dod), 32)
	}

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val)

	if vDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)

		leading := uint8(bits.LeadingZeros64(vDelta))
		trailing := uint8(bits.TrailingZeros64(vDelta))

		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 32 {
			leading = 31
		}

		// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
		if s.leading != ^uint8(0) && leading >= s.leading && trailing >= s.trailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(vDelta>>s.trailing, 64-int(s.leading)-int(s.trailing))
		} else {
			s.leading, s.trailing = leading, trailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(leading), 5)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			sigbits := 64 - leading - trailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(vDelta>>trailing, int(sigbits))
		}
	}

	s.tDelta = tDelta
	s.t = t
	s.val = v

}

// Iter lets you iterate over a series.  It is not concurrency-safe.
func (s *Series) Iter() *Iter {
	s.Lock()
	w := s.bw.clone()
	s.Unlock()

	finish(w)
	iter, _ := bstreamIterator(w)
	return iter
}

// Iter lets you iterate over a series.  It is not concurrency-safe.
type Iter struct {
	T0 uint32

	T   uint32
	Val float64

	Br       bstream
	Leading  uint8
	Trailing uint8

	Finished bool

	TDelta uint32
	Error  error
}

func bstreamIterator(br *bstream) (*Iter, error) {

	br.count = 8

	t0, err := br.readBits(32)
	if err != nil {
		return nil, err
	}

	return &Iter{
		T0: uint32(t0),
		Br: *br,
	}, nil
}

// NewIterator for the series
func NewIterator(b []byte) (*Iter, error) {
	return bstreamIterator(newBReader(b))
}

// Next iteration of the series iterator
func (it *Iter) Next() bool {

	if it.Error != nil || it.Finished {
		return false
	}

	if it.T == 0 {
		// read first t and v
		tDelta, err := it.Br.readBits(14)
		if err != nil {
			it.Error = err
			return false
		}
		it.TDelta = uint32(tDelta)
		it.T = it.T0 + it.TDelta
		v, err := it.Br.readBits(64)
		if err != nil {
			it.Error = err
			return false
		}

		it.Val = math.Float64frombits(v)

		return true
	}

	// read delta-of-delta
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.Br.readBit()
		if err != nil {
			it.Error = err
			return false
		}
		if bit == zero {
			break
		}
		d |= 1
	}

	var dod int32
	var sz uint
	switch d {
	case 0x00:
		// dod == 0
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		bits, err := it.Br.readBits(32)
		if err != nil {
			it.Error = err
			return false
		}

		// end of stream
		if bits == 0xffffffff {
			it.Finished = true
			return false
		}

		dod = int32(bits)
	}

	if sz != 0 {
		bits, err := it.Br.readBits(int(sz))
		if err != nil {
			it.Error = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int32(bits)
	}

	tDelta := it.TDelta + uint32(dod)

	it.TDelta = tDelta
	it.T = it.T + it.TDelta

	// read compressed value
	bit, err := it.Br.readBit()
	if err != nil {
		it.Error = err
		return false
	}

	if bit == zero {
		// it.Val = it.Val
	} else {
		bit, itErr := it.Br.readBit()
		if itErr != nil {
			it.Error = err
			return false
		}
		if bit == zero {
			// reuse leading/trailing zero bits
			// it.Leading, it.Trailing = it.Leading, it.Trailing
		} else {
			bits, err := it.Br.readBits(5)
			if err != nil {
				it.Error = err
				return false
			}
			it.Leading = uint8(bits)

			bits, err = it.Br.readBits(6)
			if err != nil {
				it.Error = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.Trailing = 64 - it.Leading - mbits
		}

		mbits := int(64 - it.Leading - it.Trailing)
		bits, err := it.Br.readBits(mbits)
		if err != nil {
			it.Error = err
			return false
		}
		vbits := math.Float64bits(it.Val)
		vbits ^= (bits << it.Trailing)
		it.Val = math.Float64frombits(vbits)
	}

	return true
}

// Values at the current iterator position
func (it *Iter) Values() (uint32, float64) {
	return it.T, it.Val
}

// Err error at the current iterator position
func (it *Iter) Err() error {
	return it.Error
}

type errMarshal struct {
	w   io.Writer
	r   io.Reader
	err error
}

func (em *errMarshal) write(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Write(em.w, binary.BigEndian, t)
}

func (em *errMarshal) read(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Read(em.r, binary.BigEndian, t)
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (s *Series) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	em := &errMarshal{w: buf}
	em.write(s.T0)
	em.write(s.leading)
	em.write(s.t)
	em.write(s.tDelta)
	em.write(s.trailing)
	em.write(s.val)
	bStream, err := s.bw.MarshalBinary()
	if err != nil {
		return nil, err
	}
	em.write(bStream)
	if em.err != nil {
		return nil, em.err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (s *Series) UnmarshalBinary(b []byte) error {
	buf := bytes.NewReader(b)
	em := &errMarshal{r: buf}
	em.read(&s.T0)
	em.read(&s.leading)
	em.read(&s.t)
	em.read(&s.tDelta)
	em.read(&s.trailing)
	em.read(&s.val)
	outBuf := make([]byte, buf.Len())
	em.read(outBuf)
	err := s.bw.UnmarshalBinary(outBuf)
	if err != nil {
		return err
	}
	if em.err != nil {
		return em.err
	}
	return nil
}
