// ── Client-side QR Code Generator Engine (Zero Network Dependencies) ──────
var qrcodegen = (function() {
	function QrCode(version, errorCorrectionLevel, dataCodewords, msk) {
		if (version < QrCode.MIN_VERSION || version > QrCode.MAX_VERSION)
			throw new RangeError("Version value out of range");
		if (msk < -1 || msk > 7)
			throw new RangeError("Mask value out of range");
		this.version = version;
		this.errorCorrectionLevel = errorCorrectionLevel;
		this.size = version * 4 + 17;
		
		const modules = [];
		const isFunction = [];
		for (let y = 0; y < this.size; y++) {
			modules.push([]);
			isFunction.push([]);
			for (let x = 0; x < this.size; x++) {
				modules[y].push(false);
				isFunction[y].push(false);
			}
		}
		
		const setFunctionModule = (x, y, isDark) => {
			modules[y][x] = isDark;
			isFunction[y][x] = true;
		};
		
		this.drawFinderPattern = function(x, y) {
			for (let dy = -1; dy <= 7; dy++) {
				for (let dx = -1; dx <= 7; dx++) {
					const dist = Math.max(Math.abs(dx - 3), Math.abs(dy - 3));
					const xx = x + dx, yy = y + dy;
					if (0 <= xx && xx < this.size && 0 <= yy && yy < this.size)
						setFunctionModule(xx, yy, dist !== 2 && dist !== 4);
				}
			}
		};
		
		this.drawFinderPattern(0, 0);
		this.drawFinderPattern(this.size - 7, 0);
		this.drawFinderPattern(0, this.size - 7);
		
		const alignPatPos = this.getAlignmentPatternPositions();
		const numAlign = alignPatPos.length;
		for (let i = 0; i < numAlign; i++) {
			for (let j = 0; j < numAlign; j++) {
				if ((i === 0 && j === 0) || (i === 0 && j === numAlign - 1) || (i === numAlign - 1 && j === 0))
					continue;
				const x = alignPatPos[i], y = alignPatPos[j];
				for (let dy = -2; dy <= 2; dy++) {
					for (let dx = -2; dx <= 2; dx++)
						setFunctionModule(x + dx, y + dy, Math.max(Math.abs(dx), Math.abs(dy)) !== 1);
				}
			}
		}
		
		for (let i = 0; i < this.size; i++) {
			setFunctionModule(i, 6, i % 2 === 0);
			setFunctionModule(6, i, i % 2 === 0);
		}
		
		setFunctionModule(8, this.size - 8, true);
		for (let i = 0; i < 8; i++) {
			setFunctionModule(8, i, false);
			setFunctionModule(i, 8, false);
			setFunctionModule(this.size - 1 - i, 8, false);
			setFunctionModule(8, this.size - 1 - i, false);
		}
		setFunctionModule(8, 8, false);
		if (this.version >= 7) {
			for (let i = 0; i < 6; i++) {
				for (let j = 0; j < 3; j++) {
					setFunctionModule(this.size - 11 + j, i, false);
					setFunctionModule(i, this.size - 11 + j, false);
				}
			}
		}
		
		const allCodewords = this.addEccAndInterleave(dataCodewords);
		
		if (msk === -1) {
			let minPenalty = 1e9;
			for (let i = 0; i < 8; i++) {
				this.drawCodewords(allCodewords, i, modules, isFunction);
				this.drawFormatBits(i, modules, isFunction);
				const penalty = this.getPenaltyScore(modules);
				if (penalty < minPenalty) {
					msk = i;
					minPenalty = penalty;
				}
			}
		}
		this.mask = msk;
		this.drawCodewords(allCodewords, msk, modules, isFunction);
		this.drawFormatBits(msk, modules, isFunction);
		if (this.version >= 7)
			this.drawVersion(modules, isFunction);
			
		this.getModule = function(x, y) {
			return 0 <= x && x < this.size && 0 <= y && y < this.size && modules[y][x];
		};
	}
	
	QrCode.MIN_VERSION = 1;
	QrCode.MAX_VERSION = 40;
	
	QrCode.Ecc = {
		LOW     : {ordinal: 0, formatBits: 1},
		MEDIUM  : {ordinal: 1, formatBits: 0},
		QUARTILE: {ordinal: 2, formatBits: 3},
		HIGH    : {ordinal: 3, formatBits: 2},
	};
	
	QrCode.encodeText = function(text, ecc) {
		const encoder = typeof TextEncoder !== 'undefined' ? new TextEncoder() : { encode: (s) => Buffer.from(s, 'utf8') };
		const seg = QrSegment.makeBytes(encoder.encode(text));
		return QrCode.encodeSegments([seg], ecc);
	};
	
	QrCode.encodeSegments = function(segs, ecc, minVersion, maxVersion, mask) {
		if (minVersion === undefined) minVersion = 1;
		if (maxVersion === undefined) maxVersion = 40;
		if (mask === undefined) mask = -1;
		
		let version, dataUsedBits;
		for (version = minVersion; ; version++) {
			const dataCapacityBits = QrCode.getNumDataCodewords(version, ecc) * 8;
			dataUsedBits = QrSegment.getTotalBits(segs, version);
			if (dataUsedBits <= dataCapacityBits)
				break;
			if (version >= maxVersion)
				throw new RangeError("Data too long for QR code");
		}
		
		const bitBuf = [];
		for (let i = 0; i < segs.length; i++) {
			const seg = segs[i];
			appendBits(seg.mode.modeBits, 4, bitBuf);
			appendBits(seg.numChars, seg.mode.numCharCountBits(version), bitBuf);
			for (let j = 0; j < seg.getData().length; j++) {
				bitBuf.push(seg.getData()[j]);
			}
		}
		
		const padCapacityBits = QrCode.getNumDataCodewords(version, ecc) * 8;
		appendBits(0, Math.min(4, padCapacityBits - bitBuf.length), bitBuf);
		appendBits(0, (8 - bitBuf.length % 8) % 8, bitBuf);
		for (let padByte = 0xEC; bitBuf.length < padCapacityBits; padByte ^= 0xEC ^ 0x11)
			appendBits(padByte, 8, bitBuf);
			
		const bytes = [];
		while (bytes.length * 8 < bitBuf.length) {
			let b = 0;
			for (let i = 0; i < 8; i++)
				b = (b << 1) | bitBuf[bytes.length * 8 + i];
			bytes.push(b);
		}
		return new QrCode(version, ecc, bytes, mask);
	};
	
	QrCode.prototype.getAlignmentPatternPositions = function() {
		if (this.version === 1) return [];
		const num = Math.floor(this.version / 7) + 2;
		const step = (this.version === 32) ? 26 : Math.ceil((this.version * 4 + 4) / (num * 2 - 2)) * 2;
		const result = [6];
		for (let pos = this.size - 7; result.length < num; pos -= step)
			result.splice(1, 0, pos);
		return result;
	};
	
	QrCode.prototype.drawCodewords = function(allCodewords, msk, modules, isFunction) {
		let i = 0;
		for (let right = this.size - 1; right >= 1; right -= 2) {
			if (right === 6) right = 5;
			for (let vert = 0; vert < this.size; vert++) {
				for (let j = 0; j < 2; j++) {
					const x = right - j;
					const upward = ((right + 1) & 2) === 0;
					const y = upward ? this.size - 1 - vert : vert;
					if (!isFunction[y][x] && i < allCodewords.length * 8) {
						const bit = ((allCodewords[i >>> 3] >>> (7 - (i & 7))) & 1) !== 0;
						const invert = QrCode.getMaskBit(msk, x, y);
						modules[y][x] = bit ^ invert;
						i++;
					}
				}
			}
		}
	};
	
	QrCode.prototype.drawFormatBits = function(msk, modules, isFunction) {
		const data = (this.errorCorrectionLevel.formatBits << 3) | msk;
		let rem = data;
		for (let i = 0; i < 10; i++) rem = (rem << 1) ^ ((rem >>> 9) * 0x537);
		const bits = ((data << 10) | rem) ^ 0x5412;
		
		for (let i = 0; i <= 5; i++) modules[8][i] = ((bits >>> i) & 1) !== 0;
		modules[8][6] = ((bits >>> 6) & 1) !== 0;
		modules[8][7] = ((bits >>> 7) & 1) !== 0;
		modules[8][8] = ((bits >>> 8) & 1) !== 0;
		modules[7][8] = ((bits >>> 9) & 1) !== 0;
		for (let i = 10; i < 15; i++) modules[14 - i][8] = ((bits >>> i) & 1) !== 0;
		
		for (let i = 0; i < 8; i++) modules[this.size - 1 - i][8] = ((bits >>> i) & 1) !== 0;
		for (let i = 8; i < 15; i++) modules[8][this.size - 15 + i] = ((bits >>> i) & 1) !== 0;
	};
	
	QrCode.prototype.drawVersion = function(modules, isFunction) {
		let rem = this.version;
		for (let i = 0; i < 12; i++) rem = (rem << 1) ^ ((rem >>> 11) * 0x1F25);
		const bits = (this.version << 12) | rem;
		for (let i = 0; i < 18; i++) {
			const bit = ((bits >>> i) & 1) !== 0;
			const a = this.size - 11 + (i % 3);
			const b = Math.floor(i / 3);
			modules[b][a] = bit;
			modules[a][b] = bit;
		}
	};
	
	QrCode.prototype.addEccAndInterleave = function(data) {
		const numBlocks = QrCode.NUM_ERROR_CORRECTION_BLOCKS[this.errorCorrectionLevel.ordinal][this.version];
		const blockEccLen = QrCode.ECC_CODEWORDS_PER_BLOCK[this.errorCorrectionLevel.ordinal][this.version];
		const rawCodewords = QrCode.getNumRawDataCodewords(this.version);
		const numShortBlocks = numBlocks - rawCodewords % numBlocks;
		const shortBlockLen = Math.floor(rawCodewords / numBlocks);
		
		const blocks = [];
		const rsDiv = QrCode.reedSolomonComputeDivisor(blockEccLen);
		for (let i = 0, k = 0; i < numBlocks; i++) {
			const dat = data.slice(k, k + shortBlockLen + (i < numShortBlocks ? 0 : 1));
			k += dat.length;
			const ecc = QrCode.reedSolomonComputeRemainder(dat, rsDiv);
			if (i < numShortBlocks) dat.push(0);
			blocks.push({data: dat, ecc: ecc});
		}
		
		const result = [];
		for (let i = 0; i < shortBlockLen + 1; i++) {
			for (let j = 0; j < numBlocks; j++) {
				if (i < shortBlockLen || j >= numShortBlocks) result.push(blocks[j].data[i]);
			}
		}
		for (let i = 0; i < blockEccLen; i++) {
			for (let j = 0; j < numBlocks; j++) result.push(blocks[j].ecc[i]);
		}
		return result;
	};
	
	QrCode.getMaskBit = function(msk, x, y) {
		switch (msk) {
			case 0: return (x + y) % 2 === 0;
			case 1: return y % 2 === 0;
			case 2: return x % 3 === 0;
			case 3: return (x + y) % 3 === 0;
			case 4: return (Math.floor(x / 3) + Math.floor(y / 2)) % 2 === 0;
			case 5: return (x * y) % 2 + (x * y) % 3 === 0;
			case 6: return ((x * y) % 2 + (x * y) % 3) % 2 === 0;
			case 7: return ((x * y) % 3 + (x + y) % 2) % 2 === 0;
			default: return false;
		}
	};
	
	QrCode.prototype.getPenaltyScore = function(modules) {
		let result = 0;
		for (let y = 0; y < this.size; y++) {
			let runColor = false, runX = 0;
			for (let x = 0; x < this.size; x++) {
				if (modules[y][x] === runColor) {
					runX++;
					if (runX === 5) result += 3;
					else if (runX > 5) result++;
				} else {
					runColor = modules[y][x];
					runX = 1;
				}
			}
		}
		for (let x = 0; x < this.size; x++) {
			let runColor = false, runY = 0;
			for (let y = 0; y < this.size; y++) {
				if (modules[y][x] === runColor) {
					runY++;
					if (runY === 5) result += 3;
					else if (runY > 5) result++;
				} else {
					runColor = modules[y][x];
					runY = 1;
				}
			}
		}
		return result;
	};
	
	QrCode.reedSolomonComputeDivisor = function(degree) {
		const result = [];
		for (let i = 0; i < degree - 1; i++) result.push(0);
		result.push(1);
		let root = 1;
		for (let i = 0; i < degree; i++) {
			for (let j = 0; j < result.length; j++) {
				result[j] = QrCode.reedSolomonMultiply(result[j], root);
				if (j + 1 < result.length) result[j] ^= result[j + 1];
			}
			root = QrCode.reedSolomonMultiply(root, 0x02);
		}
		return result;
	};
	
	QrCode.reedSolomonComputeRemainder = function(data, divisor) {
		const result = divisor.map(() => 0);
		for (let i = 0; i < data.length; i++) {
			const factor = data[i] ^ result.shift();
			result.push(0);
			for (let j = 0; j < divisor.length; j++)
				result[j] ^= QrCode.reedSolomonMultiply(divisor[j], factor);
		}
		return result;
	};
	
	QrCode.reedSolomonMultiply = function(x, y) {
		let z = 0;
		for (let i = 7; i >= 0; i--) {
			z = (z << 1) ^ ((z >>> 7) * 0x11D);
			z ^= ((y >>> i) & 1) * x;
		}
		return z;
	};
	
	QrCode.getNumDataCodewords = function(ver, ecl) {
		return Math.floor(QrCode.getNumRawDataCodewords(ver) / 8) -
			QrCode.ECC_CODEWORDS_PER_BLOCK[ecl.ordinal][ver] *
			QrCode.NUM_ERROR_CORRECTION_BLOCKS[ecl.ordinal][ver];
	};
	
	QrCode.getNumRawDataCodewords = function(ver) {
		const size = ver * 4 + 17;
		let numUnits = size * size - 3 * 64 - 2 * (size - 16) + 9;
		if (ver >= 2) {
			const numAlign = Math.floor(ver / 7) + 2;
			numUnits -= (numAlign * numAlign - 3) * 25;
			if (ver >= 7) numUnits -= 36;
		}
		return numUnits;
	};
	
	QrCode.ECC_CODEWORDS_PER_BLOCK = [
		[0, 7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28],
		[0, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26],
		[0, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30],
		[0, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 28, 28]
	];
	
	QrCode.NUM_ERROR_CORRECTION_BLOCKS = [
		[0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 6, 6, 6, 7, 8],
		[0, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16],
		[0, 1, 1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 10, 12, 16, 12, 17, 16, 18, 21, 20],
		[0, 1, 1, 2, 4, 4, 4, 5, 6, 8, 8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25]
	];
	
	function QrSegment(mode, numChars, data) {
		this.mode = mode;
		this.numChars = numChars;
		this.getData = function() { return data.slice(); };
	}
	
	QrSegment.Mode = {
		BYTE: {modeBits: 0x4, numCharCountBits: function(ver) { return ver < 10 ? 8 : 16; }}
	};
	
	QrSegment.makeBytes = function(data) {
		const bitBuf = [];
		for (let i = 0; i < data.length; i++)
			appendBits(data[i], 8, bitBuf);
		return new QrSegment(QrSegment.Mode.BYTE, data.length, bitBuf);
	};
	
	QrSegment.getTotalBits = function(segs, version) {
		let result = 0;
		for (let i = 0; i < segs.length; i++) {
			const seg = segs[i];
			const ccbits = seg.mode.numCharCountBits(version);
			if (seg.numChars >= (1 << ccbits)) return 1e9;
			result += 4 + ccbits + seg.getData().length;
		}
		return result;
	};
	
	function appendBits(val, len, bitBuf) {
		for (let i = len - 1; i >= 0; i--)
			bitBuf.push((val >>> i) & 1);
	}
	
	return { QrCode };
})();

function renderQRCode(arg1, arg2, options = {}) {
  let text = null;
  let targetElement = null;

  if (arg1 && typeof arg1 === 'object') {
    targetElement = arg1;
    text = arg2;
  } else if (arg2 && typeof arg2 === 'object') {
    targetElement = arg2;
    text = arg1;
  } else if (typeof arg1 === 'string' && typeof arg2 === 'string') {
    const el1 = document.getElementById(arg1);
    if (el1) {
      targetElement = el1;
      text = arg2;
    } else {
      const el2 = document.getElementById(arg2);
      if (el2) {
        targetElement = el2;
        text = arg1;
      } else {
        text = arg1;
      }
    }
  } else if (typeof arg1 === 'string') {
    targetElement = document.getElementById(arg1);
    text = arg2;
  }

  if (!text || !targetElement) return;

  const rawSvg = generateQRCodeSVG(text);
  const svgStr = rawSvg.replace('<path fill="#000000" d=', '<path d=');
  const tag = targetElement.tagName ? targetElement.tagName.toLowerCase() : '';

  if (tag === 'img') {
    targetElement.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgStr);
  } else if (tag === 'svg') {
    const vbMatch = svgStr.match(/viewBox="([^"]+)"/);
    if (vbMatch) {
      targetElement.setAttribute('viewBox', vbMatch[1]);
    }
    const innerMatch = svgStr.match(/<svg[^>]*>([\s\S]*)<\/svg>/);
    if (innerMatch) {
      targetElement.innerHTML = innerMatch[1];
    }
  } else if (tag === 'canvas') {
    targetElement.width = targetElement.width || 256;
    targetElement.height = targetElement.height || 256;
    const qrc = (typeof window !== 'undefined' && window.qrcodegen) ? window.qrcodegen : (typeof globalThis !== 'undefined' && globalThis.qrcodegen ? globalThis.qrcodegen : (typeof qrcodegen !== 'undefined' ? qrcodegen : null));
    if (qrc && qrc.QrCode) {
      try {
        const qr = qrc.QrCode.encodeText(text, qrc.QrCode.Ecc.MEDIUM);
        renderQRToCanvas(qr, targetElement, 4);
      } catch (e) {
        const ctx = targetElement.getContext ? targetElement.getContext('2d') : null;
        if (ctx) {
          ctx.fillStyle = '#ffffff';
          ctx.fillRect(0, 0, targetElement.width, targetElement.height);
        }
      }
    } else {
      const ctx = targetElement.getContext ? targetElement.getContext('2d') : null;
      if (ctx) {
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, targetElement.width, targetElement.height);
      }
    }
  } else {
    targetElement.innerHTML = svgStr;
  }
}

function renderQRElements(url, canvasId, imgId) {
  const canvasEl = document.getElementById(canvasId);
  if (canvasEl) {
    renderQRCode(url, canvasEl);
  }
  const imgEl = document.getElementById(imgId);
  if (imgEl) {
    renderQRCode(url, imgEl);
  }
}

function getBackendURL() {
  const params = new URLSearchParams(window.location.search);
  let backend = params.get('backend') || params.get('b') || window.GAZE_BACKEND_URL || window.BACKEND_URL || '';
  if (!backend) {
    backend = window.location.origin;
    if (backend.includes('vercel.app') || backend.startsWith('file://')) {
      backend = 'https://beamshare.onrender.com';
    }
  }
  if (backend && backend.endsWith('/')) {
    backend = backend.slice(0, -1);
  }
  return backend;
}

function apiPath(path) {
  const backend = getBackendURL();
  const params = new URLSearchParams(window.location.search);
  const s = params.get('s');
  let fullPath = path;
  if (s) {
    fullPath = path.includes('?') ? path + '&s=' + s : path + '?s=' + s;
  }
  if (backend) {
    return backend + fullPath;
  }
  return fullPath;
}
/**
 * app.js — Gaze Receiver (Phase 4 & 5: Direct-to-Disk + Live Pipe)
 *
 * Connection strategy (tries in order):
 *   1. WebRTC P2P via RTCDataChannel (works through client-isolated Wi-Fi)
 *   2. Direct HTTP fallback (same LAN, no firewall issues)
 *
 * Direct-to-Disk:
 *   - Uses the FileSystem Writable Stream API if available.
 *   - Saves chunks directly to disk without bloating RAM.
 *   - Falls back to Blob memory buffers if picker is denied or unsupported.
 *
 * Live Pipe:
 *   - Renders a terminal simulator scrolling live output from standard input.
 *   - Implements both WebRTC text data pipe and SSE (Server-Sent Events) live log bridge.
 */

'use strict';


// ── Local QR Code Generator & Renderer ─────────────────────────────────────
"use strict";
// var qrcodegen;
(function (qrcodegen) {
 class QrCode {
 constructor(
 version, 
 errorCorrectionLevel, dataCodewords, msk) {
 this.version = version;
 this.errorCorrectionLevel = errorCorrectionLevel;
 this.modules = [];
 this.isFunction = [];
 if (version < QrCode.MIN_VERSION || version > QrCode.MAX_VERSION)
 throw new RangeError("Version value out of range");
 if (msk < -1 || msk > 7)
 throw new RangeError("Mask value out of range");
 this.size = version * 4 + 17;
 let row = [];
 for (let i = 0; i < this.size; i++)
 row.push(false);
 for (let i = 0; i < this.size; i++) {
 this.modules.push(row.slice()); 
 this.isFunction.push(row.slice());
 }
 this.drawFunctionPatterns();
 const allCodewords = this.addEccAndInterleave(dataCodewords);
 this.drawCodewords(allCodewords);
 if (msk == -1) { 
 let minPenalty = 1000000000;
 for (let i = 0; i < 8; i++) {
 this.applyMask(i);
 this.drawFormatBits(i);
 const penalty = this.getPenaltyScore();
 if (penalty < minPenalty) {
 msk = i;
 minPenalty = penalty;
 }
 this.applyMask(i); 
 }
 }
 assert(0 <= msk && msk <= 7);
 this.mask = msk;
 this.applyMask(msk); 
 this.drawFormatBits(msk); 
 this.isFunction = [];
 }
 static encodeText(text, ecl) {
 const segs = qrcodegen.QrSegment.makeSegments(text);
 return QrCode.encodeSegments(segs, ecl);
 }
 static encodeBinary(data, ecl) {
 const seg = qrcodegen.QrSegment.makeBytes(data);
 return QrCode.encodeSegments([seg], ecl);
 }
 static encodeSegments(segs, ecl, minVersion = 1, maxVersion = 40, mask = -1, boostEcl = true) {
 if (!(QrCode.MIN_VERSION <= minVersion && minVersion <= maxVersion && maxVersion <= QrCode.MAX_VERSION)
 || mask < -1 || mask > 7)
 throw new RangeError("Invalid value");
 let version;
 let dataUsedBits;
 for (version = minVersion;; version++) {
 const dataCapacityBits = QrCode.getNumDataCodewords(version, ecl) * 8; 
 const usedBits = QrSegment.getTotalBits(segs, version);
 if (usedBits <= dataCapacityBits) {
 dataUsedBits = usedBits;
 break; 
 }
 if (version >= maxVersion) 
 throw new RangeError("Data too long");
 }
 for (const newEcl of [QrCode.Ecc.MEDIUM, QrCode.Ecc.QUARTILE, QrCode.Ecc.HIGH]) { 
 if (boostEcl && dataUsedBits <= QrCode.getNumDataCodewords(version, newEcl) * 8)
 ecl = newEcl;
 }
 let bb = [];
 for (const seg of segs) {
 appendBits(seg.mode.modeBits, 4, bb);
 appendBits(seg.numChars, seg.mode.numCharCountBits(version), bb);
 for (const b of seg.getData())
 bb.push(b);
 }
 assert(bb.length == dataUsedBits);
 const dataCapacityBits = QrCode.getNumDataCodewords(version, ecl) * 8;
 assert(bb.length <= dataCapacityBits);
 appendBits(0, Math.min(4, dataCapacityBits - bb.length), bb);
 appendBits(0, (8 - bb.length % 8) % 8, bb);
 assert(bb.length % 8 == 0);
 for (let padByte = 0xEC; bb.length < dataCapacityBits; padByte ^= 0xEC ^ 0x11)
 appendBits(padByte, 8, bb);
 let dataCodewords = [];
 while (dataCodewords.length * 8 < bb.length)
 dataCodewords.push(0);
 bb.forEach((b, i) => dataCodewords[i >>> 3] |= b << (7 - (i & 7)));
 return new QrCode(version, ecl, dataCodewords, mask);
 }
 getModule(x, y) {
 return 0 <= x && x < this.size && 0 <= y && y < this.size && this.modules[y][x];
 }
 drawFunctionPatterns() {
 for (let i = 0; i < this.size; i++) {
 this.setFunctionModule(6, i, i % 2 == 0);
 this.setFunctionModule(i, 6, i % 2 == 0);
 }
 this.drawFinderPattern(3, 3);
 this.drawFinderPattern(this.size - 4, 3);
 this.drawFinderPattern(3, this.size - 4);
 const alignPatPos = this.getAlignmentPatternPositions();
 const numAlign = alignPatPos.length;
 for (let i = 0; i < numAlign; i++) {
 for (let j = 0; j < numAlign; j++) {
 if (!(i == 0 && j == 0 || i == 0 && j == numAlign - 1 || i == numAlign - 1 && j == 0))
 this.drawAlignmentPattern(alignPatPos[i], alignPatPos[j]);
 }
 }
 this.drawFormatBits(0); 
 this.drawVersion();
 }
 drawFormatBits(mask) {
 const data = this.errorCorrectionLevel.formatBits << 3 | mask; 
 let rem = data;
 for (let i = 0; i < 10; i++)
 rem = (rem << 1) ^ ((rem >>> 9) * 0x537);
 const bits = (data << 10 | rem) ^ 0x5412; 
 assert(bits >>> 15 == 0);
 for (let i = 0; i <= 5; i++)
 this.setFunctionModule(8, i, getBit(bits, i));
 this.setFunctionModule(8, 7, getBit(bits, 6));
 this.setFunctionModule(8, 8, getBit(bits, 7));
 this.setFunctionModule(7, 8, getBit(bits, 8));
 for (let i = 9; i < 15; i++)
 this.setFunctionModule(14 - i, 8, getBit(bits, i));
 for (let i = 0; i < 8; i++)
 this.setFunctionModule(this.size - 1 - i, 8, getBit(bits, i));
 for (let i = 8; i < 15; i++)
 this.setFunctionModule(8, this.size - 15 + i, getBit(bits, i));
 this.setFunctionModule(8, this.size - 8, true); 
 }
 drawVersion() {
 if (this.version < 7)
 return;
 let rem = this.version; 
 for (let i = 0; i < 12; i++)
 rem = (rem << 1) ^ ((rem >>> 11) * 0x1F25);
 const bits = this.version << 12 | rem; 
 assert(bits >>> 18 == 0);
 for (let i = 0; i < 18; i++) {
 const color = getBit(bits, i);
 const a = this.size - 11 + i % 3;
 const b = Math.floor(i / 3);
 this.setFunctionModule(a, b, color);
 this.setFunctionModule(b, a, color);
 }
 }
 drawFinderPattern(x, y) {
 for (let dy = -4; dy <= 4; dy++) {
 for (let dx = -4; dx <= 4; dx++) {
 const dist = Math.max(Math.abs(dx), Math.abs(dy)); 
 const xx = x + dx;
 const yy = y + dy;
 if (0 <= xx && xx < this.size && 0 <= yy && yy < this.size)
 this.setFunctionModule(xx, yy, dist != 2 && dist != 4);
 }
 }
 }
 drawAlignmentPattern(x, y) {
 for (let dy = -2; dy <= 2; dy++) {
 for (let dx = -2; dx <= 2; dx++)
 this.setFunctionModule(x + dx, y + dy, Math.max(Math.abs(dx), Math.abs(dy)) != 1);
 }
 }
 setFunctionModule(x, y, isDark) {
 this.modules[y][x] = isDark;
 this.isFunction[y][x] = true;
 }
 addEccAndInterleave(data) {
 const ver = this.version;
 const ecl = this.errorCorrectionLevel;
 if (data.length != QrCode.getNumDataCodewords(ver, ecl))
 throw new RangeError("Invalid argument");
 const numBlocks = QrCode.NUM_ERROR_CORRECTION_BLOCKS[ecl.ordinal][ver];
 const blockEccLen = QrCode.ECC_CODEWORDS_PER_BLOCK[ecl.ordinal][ver];
 const rawCodewords = Math.floor(QrCode.getNumRawDataModules(ver) / 8);
 const numShortBlocks = numBlocks - rawCodewords % numBlocks;
 const shortBlockLen = Math.floor(rawCodewords / numBlocks);
 let blocks = [];
 const rsDiv = QrCode.reedSolomonComputeDivisor(blockEccLen);
 for (let i = 0, k = 0; i < numBlocks; i++) {
 let dat = data.slice(k, k + shortBlockLen - blockEccLen + (i < numShortBlocks ? 0 : 1));
 k += dat.length;
 const ecc = QrCode.reedSolomonComputeRemainder(dat, rsDiv);
 if (i < numShortBlocks)
 dat.push(0);
 blocks.push(dat.concat(ecc));
 }
 let result = [];
 for (let i = 0; i < blocks[0].length; i++) {
 blocks.forEach((block, j) => {
 if (i != shortBlockLen - blockEccLen || j >= numShortBlocks)
 result.push(block[i]);
 });
 }
 assert(result.length == rawCodewords);
 return result;
 }
 drawCodewords(data) {
 if (data.length != Math.floor(QrCode.getNumRawDataModules(this.version) / 8))
 throw new RangeError("Invalid argument");
 let i = 0; 
 for (let right = this.size - 1; right >= 1; right -= 2) { 
 if (right == 6)
 right = 5;
 for (let vert = 0; vert < this.size; vert++) { 
 for (let j = 0; j < 2; j++) {
 const x = right - j; 
 const upward = ((right + 1) & 2) == 0;
 const y = upward ? this.size - 1 - vert : vert; 
 if (!this.isFunction[y][x] && i < data.length * 8) {
 this.modules[y][x] = getBit(data[i >>> 3], 7 - (i & 7));
 i++;
 }
 }
 }
 }
 assert(i == data.length * 8);
 }
 applyMask(mask) {
 if (mask < 0 || mask > 7)
 throw new RangeError("Mask value out of range");
 for (let y = 0; y < this.size; y++) {
 for (let x = 0; x < this.size; x++) {
 let invert;
 switch (mask) {
 case 0:
 invert = (x + y) % 2 == 0;
 break;
 case 1:
 invert = y % 2 == 0;
 break;
 case 2:
 invert = x % 3 == 0;
 break;
 case 3:
 invert = (x + y) % 3 == 0;
 break;
 case 4:
 invert = (Math.floor(x / 3) + Math.floor(y / 2)) % 2 == 0;
 break;
 case 5:
 invert = x * y % 2 + x * y % 3 == 0;
 break;
 case 6:
 invert = (x * y % 2 + x * y % 3) % 2 == 0;
 break;
 case 7:
 invert = ((x + y) % 2 + x * y % 3) % 2 == 0;
 break;
 default: throw new Error("Unreachable");
 }
 if (!this.isFunction[y][x] && invert)
 this.modules[y][x] = !this.modules[y][x];
 }
 }
 }
 getPenaltyScore() {
 let result = 0;
 for (let y = 0; y < this.size; y++) {
 let runColor = false;
 let runX = 0;
 let runHistory = [0, 0, 0, 0, 0, 0, 0];
 for (let x = 0; x < this.size; x++) {
 if (this.modules[y][x] == runColor) {
 runX++;
 if (runX == 5)
 result += QrCode.PENALTY_N1;
 else if (runX > 5)
 result++;
 }
 else {
 this.finderPenaltyAddHistory(runX, runHistory);
 if (!runColor)
 result += this.finderPenaltyCountPatterns(runHistory) * QrCode.PENALTY_N3;
 runColor = this.modules[y][x];
 runX = 1;
 }
 }
 result += this.finderPenaltyTerminateAndCount(runColor, runX, runHistory) * QrCode.PENALTY_N3;
 }
 for (let x = 0; x < this.size; x++) {
 let runColor = false;
 let runY = 0;
 let runHistory = [0, 0, 0, 0, 0, 0, 0];
 for (let y = 0; y < this.size; y++) {
 if (this.modules[y][x] == runColor) {
 runY++;
 if (runY == 5)
 result += QrCode.PENALTY_N1;
 else if (runY > 5)
 result++;
 }
 else {
 this.finderPenaltyAddHistory(runY, runHistory);
 if (!runColor)
 result += this.finderPenaltyCountPatterns(runHistory) * QrCode.PENALTY_N3;
 runColor = this.modules[y][x];
 runY = 1;
 }
 }
 result += this.finderPenaltyTerminateAndCount(runColor, runY, runHistory) * QrCode.PENALTY_N3;
 }
 for (let y = 0; y < this.size - 1; y++) {
 for (let x = 0; x < this.size - 1; x++) {
 const color = this.modules[y][x];
 if (color == this.modules[y][x + 1] &&
 color == this.modules[y + 1][x] &&
 color == this.modules[y + 1][x + 1])
 result += QrCode.PENALTY_N2;
 }
 }
 let dark = 0;
 for (const row of this.modules)
 dark = row.reduce((sum, color) => sum + (color ? 1 : 0), dark);
 const total = this.size * this.size; 
 const k = Math.ceil(Math.abs(dark * 20 - total * 10) / total) - 1;
 assert(0 <= k && k <= 9);
 result += k * QrCode.PENALTY_N4;
 assert(0 <= result && result <= 2568888); 
 return result;
 }
 getAlignmentPatternPositions() {
 if (this.version == 1)
 return [];
 else {
 const numAlign = Math.floor(this.version / 7) + 2;
 const step = (this.version == 32) ? 26 :
 Math.ceil((this.version * 4 + 4) / (numAlign * 2 - 2)) * 2;
 let result = [6];
 for (let pos = this.size - 7; result.length < numAlign; pos -= step)
 result.splice(1, 0, pos);
 return result;
 }
 }
 static getNumRawDataModules(ver) {
 if (ver < QrCode.MIN_VERSION || ver > QrCode.MAX_VERSION)
 throw new RangeError("Version number out of range");
 let result = (16 * ver + 128) * ver + 64;
 if (ver >= 2) {
 const numAlign = Math.floor(ver / 7) + 2;
 result -= (25 * numAlign - 10) * numAlign - 55;
 if (ver >= 7)
 result -= 36;
 }
 assert(208 <= result && result <= 29648);
 return result;
 }
 static getNumDataCodewords(ver, ecl) {
 return Math.floor(QrCode.getNumRawDataModules(ver) / 8) -
 QrCode.ECC_CODEWORDS_PER_BLOCK[ecl.ordinal][ver] *
 QrCode.NUM_ERROR_CORRECTION_BLOCKS[ecl.ordinal][ver];
 }
 static reedSolomonComputeDivisor(degree) {
 if (degree < 1 || degree > 255)
 throw new RangeError("Degree out of range");
 let result = [];
 for (let i = 0; i < degree - 1; i++)
 result.push(0);
 result.push(1); 
 let root = 1;
 for (let i = 0; i < degree; i++) {
 for (let j = 0; j < result.length; j++) {
 result[j] = QrCode.reedSolomonMultiply(result[j], root);
 if (j + 1 < result.length)
 result[j] ^= result[j + 1];
 }
 root = QrCode.reedSolomonMultiply(root, 0x02);
 }
 return result;
 }
 static reedSolomonComputeRemainder(data, divisor) {
 let result = divisor.map(_ => 0);
 for (const b of data) { 
 const factor = b ^ result.shift();
 result.push(0);
 divisor.forEach((coef, i) => result[i] ^= QrCode.reedSolomonMultiply(coef, factor));
 }
 return result;
 }
 static reedSolomonMultiply(x, y) {
 if (x >>> 8 != 0 || y >>> 8 != 0)
 throw new RangeError("Byte out of range");
 let z = 0;
 for (let i = 7; i >= 0; i--) {
 z = (z << 1) ^ ((z >>> 7) * 0x11D);
 z ^= ((y >>> i) & 1) * x;
 }
 assert(z >>> 8 == 0);
 return z;
 }
 finderPenaltyCountPatterns(runHistory) {
 const n = runHistory[1];
 assert(n <= this.size * 3);
 const core = n > 0 && runHistory[2] == n && runHistory[3] == n * 3 && runHistory[4] == n && runHistory[5] == n;
 return (core && runHistory[0] >= n * 4 && runHistory[6] >= n ? 1 : 0)
 + (core && runHistory[6] >= n * 4 && runHistory[0] >= n ? 1 : 0);
 }
 finderPenaltyTerminateAndCount(currentRunColor, currentRunLength, runHistory) {
 if (currentRunColor) { 
 this.finderPenaltyAddHistory(currentRunLength, runHistory);
 currentRunLength = 0;
 }
 currentRunLength += this.size; 
 this.finderPenaltyAddHistory(currentRunLength, runHistory);
 return this.finderPenaltyCountPatterns(runHistory);
 }
 finderPenaltyAddHistory(currentRunLength, runHistory) {
 if (runHistory[0] == 0)
 currentRunLength += this.size; 
 runHistory.pop();
 runHistory.unshift(currentRunLength);
 }
 }
 QrCode.MIN_VERSION = 1;
 QrCode.MAX_VERSION = 40;
 QrCode.PENALTY_N1 = 3;
 QrCode.PENALTY_N2 = 3;
 QrCode.PENALTY_N3 = 40;
 QrCode.PENALTY_N4 = 10;
 QrCode.ECC_CODEWORDS_PER_BLOCK = [
 [-1, 7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28, 28, 28, 30, 30, 26, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
 [-1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26, 26, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28],
 [-1, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30, 28, 30, 30, 30, 30, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
 [-1, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 26, 28, 30, 24, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30], 
 ];
 QrCode.NUM_ERROR_CORRECTION_BLOCKS = [
 [-1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 6, 6, 6, 7, 8, 8, 9, 9, 10, 12, 12, 12, 13, 14, 15, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25],
 [-1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16, 17, 17, 18, 20, 21, 23, 25, 26, 28, 29, 31, 33, 35, 37, 38, 40, 43, 45, 47, 49],
 [-1, 1, 1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 10, 12, 16, 12, 17, 16, 18, 21, 20, 23, 23, 25, 27, 29, 34, 34, 35, 38, 40, 43, 45, 48, 51, 53, 56, 59, 62, 65, 68],
 [-1, 1, 1, 2, 4, 4, 4, 5, 6, 8, 8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25, 25, 34, 30, 32, 35, 37, 40, 42, 45, 48, 51, 54, 57, 60, 63, 66, 70, 74, 77, 81], 
 ];
 qrcodegen.QrCode = QrCode;
 function appendBits(val, len, bb) {
 if (len < 0 || len > 31 || val >>> len != 0)
 throw new RangeError("Value out of range");
 for (let i = len - 1; i >= 0; i--) 
 bb.push((val >>> i) & 1);
 }
 function getBit(x, i) {
 return ((x >>> i) & 1) != 0;
 }
 function assert(cond) {
 if (!cond)
 throw new Error("Assertion error");
 }
 class QrSegment {
 constructor(
 mode, 
 numChars, 
 bitData) {
 this.mode = mode;
 this.numChars = numChars;
 this.bitData = bitData;
 if (numChars < 0)
 throw new RangeError("Invalid argument");
 this.bitData = bitData.slice(); 
 }
 static makeBytes(data) {
 let bb = [];
 for (const b of data)
 appendBits(b, 8, bb);
 return new QrSegment(QrSegment.Mode.BYTE, data.length, bb);
 }
 static makeNumeric(digits) {
 if (!QrSegment.isNumeric(digits))
 throw new RangeError("String contains non-numeric characters");
 let bb = [];
 for (let i = 0; i < digits.length;) { 
 const n = Math.min(digits.length - i, 3);
 appendBits(parseInt(digits.substr(i, n), 10), n * 3 + 1, bb);
 i += n;
 }
 return new QrSegment(QrSegment.Mode.NUMERIC, digits.length, bb);
 }
 static makeAlphanumeric(text) {
 if (!QrSegment.isAlphanumeric(text))
 throw new RangeError("String contains unencodable characters in alphanumeric mode");
 let bb = [];
 let i;
 for (i = 0; i + 2 <= text.length; i += 2) { 
 let temp = QrSegment.ALPHANUMERIC_CHARSET.indexOf(text.charAt(i)) * 45;
 temp += QrSegment.ALPHANUMERIC_CHARSET.indexOf(text.charAt(i + 1));
 appendBits(temp, 11, bb);
 }
 if (i < text.length) 
 appendBits(QrSegment.ALPHANUMERIC_CHARSET.indexOf(text.charAt(i)), 6, bb);
 return new QrSegment(QrSegment.Mode.ALPHANUMERIC, text.length, bb);
 }
 static makeSegments(text) {
 if (text == "")
 return [];
 else if (QrSegment.isNumeric(text))
 return [QrSegment.makeNumeric(text)];
 else if (QrSegment.isAlphanumeric(text))
 return [QrSegment.makeAlphanumeric(text)];
 else
 return [QrSegment.makeBytes(QrSegment.toUtf8ByteArray(text))];
 }
 static makeEci(assignVal) {
 let bb = [];
 if (assignVal < 0)
 throw new RangeError("ECI assignment value out of range");
 else if (assignVal < (1 << 7))
 appendBits(assignVal, 8, bb);
 else if (assignVal < (1 << 14)) {
 appendBits(0b10, 2, bb);
 appendBits(assignVal, 14, bb);
 }
 else if (assignVal < 1000000) {
 appendBits(0b110, 3, bb);
 appendBits(assignVal, 21, bb);
 }
 else
 throw new RangeError("ECI assignment value out of range");
 return new QrSegment(QrSegment.Mode.ECI, 0, bb);
 }
 static isNumeric(text) {
 return QrSegment.NUMERIC_REGEX.test(text);
 }
 static isAlphanumeric(text) {
 return QrSegment.ALPHANUMERIC_REGEX.test(text);
 }
 getData() {
 return this.bitData.slice(); 
 }
 static getTotalBits(segs, version) {
 let result = 0;
 for (const seg of segs) {
 const ccbits = seg.mode.numCharCountBits(version);
 if (seg.numChars >= (1 << ccbits))
 return Infinity; 
 result += 4 + ccbits + seg.bitData.length;
 }
 return result;
 }
 static toUtf8ByteArray(str) {
 str = encodeURI(str);
 let result = [];
 for (let i = 0; i < str.length; i++) {
 if (str.charAt(i) != "%")
 result.push(str.charCodeAt(i));
 else {
 result.push(parseInt(str.substr(i + 1, 2), 16));
 i += 2;
 }
 }
 return result;
 }
 }
 QrSegment.NUMERIC_REGEX = /^[0-9]*$/;
 QrSegment.ALPHANUMERIC_REGEX = /^[A-Z0-9 $%*+.\/:-]*$/;
 QrSegment.ALPHANUMERIC_CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ $%*+-./:";
 qrcodegen.QrSegment = QrSegment;
})(qrcodegen || (qrcodegen = {}));
(function (qrcodegen) {
 var QrCode;
 (function (QrCode) {
 class Ecc {
 constructor(
 ordinal, 
 formatBits) {
 this.ordinal = ordinal;
 this.formatBits = formatBits;
 }
 }
 Ecc.LOW = new Ecc(0, 1); 
 Ecc.MEDIUM = new Ecc(1, 0); 
 Ecc.QUARTILE = new Ecc(2, 3); 
 Ecc.HIGH = new Ecc(3, 2); 
 QrCode.Ecc = Ecc;
 })(QrCode = qrcodegen.QrCode || (qrcodegen.QrCode = {}));
})(qrcodegen || (qrcodegen = {}));
(function (qrcodegen) {
 var QrSegment;
 (function (QrSegment) {
 class Mode {
 constructor(
 modeBits, 
 numBitsCharCount) {
 this.modeBits = modeBits;
 this.numBitsCharCount = numBitsCharCount;
 }
 numCharCountBits(ver) {
 return this.numBitsCharCount[Math.floor((ver + 7) / 17)];
 }
 }
 Mode.NUMERIC = new Mode(0x1, [10, 12, 14]);
 Mode.ALPHANUMERIC = new Mode(0x2, [9, 11, 13]);
 Mode.BYTE = new Mode(0x4, [8, 16, 16]);
 Mode.KANJI = new Mode(0x8, [8, 10, 12]);
 Mode.ECI = new Mode(0x7, [0, 0, 0]);
 QrSegment.Mode = Mode;
 })(QrSegment = qrcodegen.QrSegment || (qrcodegen.QrSegment = {}));
})(qrcodegen || (qrcodegen = {}));

if (typeof globalThis !== "undefined") globalThis.qrcodegen = qrcodegen;
if (typeof window !== "undefined") window.qrcodegen = qrcodegen;


function qrToSvgDataUrl(qr, border = 4) {
  const size = qr.size;
  const totalSize = size + border * 2;
  let svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ' + totalSize + ' ' + totalSize + '" width="100%" height="100%" shape-rendering="crispEdges">';
  svg += '<rect width="' + totalSize + '" height="' + totalSize + '" fill="#ffffff"/>';
  svg += '<path d="';
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      if (qr.getModule(x, y)) {
        svg += 'M' + (x + border) + ',' + (y + border) + 'h1v1h-1z ';
      }
    }
  }
  svg += '" fill="#000000"/>';
  svg += '</svg>';
  return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
}



function renderQRToCanvas(qr, canvas, border = 4) {
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error("Canvas 2D context unavailable");
  const size = qr.size;
  const totalSize = size + border * 2;
  const cellSize = Math.floor(Math.min(canvas.width, canvas.height) / totalSize) || 1;
  
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  
  ctx.fillStyle = '#000000';
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      if (qr.getModule(x, y)) {
        ctx.fillRect((x + border) * cellSize, (y + border) * cellSize, cellSize, cellSize);
      }
    }
  }
}

function showQRError(element, message) {
  if (!element) return;
  const tagName = element.tagName ? element.tagName.toLowerCase() : '';
  if (tagName === 'img') {
    element.alt = message;
    const errSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="180" height="180" viewBox="0 0 180 180"><rect width="100%" height="100%" fill="#1f2937"/><text x="50%" y="50%" font-family="sans-serif" font-size="12" fill="#ef4444" text-anchor="middle" dominant-baseline="middle">' + message + '</text></svg>';
    element.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(errSvg);
  } else {
    element.textContent = message;
  }
}

// ── Constants ─────────────────────────────────────────────────────────────────
const STUN_SERVERS    = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' }
];

function getIceServers(offer) {
  const params = new URLSearchParams(window.location.search);
  const servers = [];

  if (params.get('turn_server') || params.get('stun_server') || params.get('ice_servers')) {
    if (params.get('ice_servers')) {
      try {
        const parsedIceServers = JSON.parse(params.get('ice_servers'));
        if (Array.isArray(parsedIceServers)) {
           return parsedIceServers;
        }
      } catch (e) {
        console.warn("Failed to parse ice_servers query param", e);
      }
    }
    if (params.get('turn_server')) {
      const turnServerParams = params.getAll('turn_server');
      const turnServers = turnServerParams.flatMap(s => s.split(',')).map(s => s.trim()).filter(Boolean);
      const username = params.get('turn_username');
      const credential = params.get('turn_credential');
      servers.push({
        urls: turnServers,
        username: username || '',
        credential: credential || ''
      });
    }
    if (params.get('stun_server')) {
      const stunServerParams = params.getAll('stun_server');
      const stunServers = stunServerParams.flatMap(s => s.split(',')).map(s => s.trim()).filter(Boolean);
      servers.push({ urls: stunServers });
    }
    if (servers.length > 0) {
      return servers;
    }
  }

  return offer && offer.iceServers ? offer.iceServers : STUN_SERVERS;
}

const CIRCUMFERENCE   = 2 * Math.PI * 42; // SVG progress ring

// ── State ──────────────────────────────────────────────────────────────────────
let currentFile      = null;

/**
 * Waits for a WebRTC DataChannel's bufferedAmount to drop to or below targetThreshold.
 * Combines bufferedamountlow listener, post-registration level check, and 250ms timeout fallback.
 *
 * @param {RTCDataChannel} dc
 * @param {number} targetThreshold - Target bufferedAmount in bytes
 * @param {number} timeoutMs - Timeout fallback in milliseconds (default: 250)
 * @returns {Promise<void>}
 */
function waitForBufferedAmountLow(dc, targetThreshold = 0, timeoutMs = 250) {
  if (!dc) return Promise.resolve();
  try {
    dc.bufferedAmountLowThreshold = targetThreshold;
  } catch (e) {}

  if (dc.bufferedAmount <= targetThreshold) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    let timer = null;
    let resolved = false;

    const cleanupAndResolve = () => {
      if (resolved) return;
      resolved = true;
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
      try {
        dc.removeEventListener('bufferedamountlow', listener);
      } catch (e) {}
      resolve();
    };

    const listener = () => {
      cleanupAndResolve();
    };

    try {
      dc.addEventListener('bufferedamountlow', listener);
    } catch (e) {
      cleanupAndResolve();
      return;
    }

    // Immediate post-registration check in case threshold was crossed during callback setup
    if (dc.bufferedAmount <= targetThreshold) {
      cleanupAndResolve();
      return;
    }

    timer = setTimeout(() => {
      cleanupAndResolve();
    }, timeoutMs);
  });
}
let transferMode     = 'http';   // 'webrtc' | 'http'
let startTime        = 0;
let receivedBytes    = 0;
let totalBytes       = 0;
let receivedChunks   = [];
let isLivePipeMode   = false;



// ── ANSI Escape Handling ───────────────────────────────────────────────────────
function stripAnsi(str) {
  // Pattern to match ANSI escape codes
  return str.replace(/\x1b\[[0-9;]*m/g, '');
}

function parseAnsiToHtml(str) {
  let html = '';
  let openSpans = 0;

  // Matches ANSI escape codes and captures the code inside [ and m
  const parts = str.split(/(\x1b\[[0-9;]*m)/g);

  for (const part of parts) {
    if (part.startsWith('\x1b[')) {
      const codeStr = part.substring(2, part.length - 1);
      if (codeStr === '0' || codeStr === '') {
        while (openSpans > 0) {
          html += '</span>';
          openSpans--;
        }
      } else {
        const codes = codeStr.split(';');
        for (const code of codes) {
          if (code === '32') {
            html += '<span style="color: #a7f3d0;">';
            openSpans++;
          } else if (code === '90') {
            html += '<span style="color: var(--zinc-600);">';
            openSpans++;
          } else if (code === '31') {
             html += '<span style="color: #ef4444;">';
             openSpans++;
          }
        }
      }
    } else if (part.length > 0) {
      // Escape HTML
      const escaped = part.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      html += escaped;
    }
  }
  while (openSpans > 0) {
    html += '</span>';
    openSpans--;
  }
  return html;
}

class VirtualLogViewer {
  constructor(containerQuery, maxLines = 100000) {
    this.container = document.querySelector(containerQuery);
    if (!this.container) return;
    this.maxLines = maxLines;
    this.lines = [];
    this.filteredIndices = [];
    this.filterQuery = "";
    this.buffer = "";
    
    this.spacer = document.createElement('div');
    this.spacer.style.width = '1px';
    
    this.content = document.createElement('div');
    this.content.style.position = 'absolute';
    this.content.style.top = '0';
    this.content.style.left = '0';
    this.content.style.width = '100%';
    this.content.style.height = '100%';
    
    this.container.innerHTML = '';
    this.container.appendChild(this.spacer);
    this.container.appendChild(this.content);
    
    const testLine = document.createElement('div');
    testLine.textContent = 'A';
    testLine.className = 'log-line';
    testLine.style.fontFamily = "'Geist Mono', monospace";
    testLine.style.fontSize = "0.8125rem";
    testLine.style.lineHeight = "1.5";
    this.content.appendChild(testLine);
    this.lineHeight = testLine.getBoundingClientRect().height || 19.5;
    this.content.removeChild(testLine);
    
    this.visibleNodes = new Map();
    this.isAutoScroll = true;
    
    this.onScroll = () => {
      const maxScroll = this.container.scrollHeight - this.container.clientHeight;
      if (maxScroll - this.container.scrollTop < 10) {
         this.isAutoScroll = true;
      } else {
         this.isAutoScroll = false;
      }
      this.updatePauseButton();
      this.render();
    };
    
    this.container.addEventListener('scroll', this.onScroll, { passive: true });
    
    if (window.ResizeObserver) {
      this.ro = new ResizeObserver(() => this.render());
      this.ro.observe(this.container);
    }
  }

  updatePauseButton() {
    const btn = document.getElementById('btn-terminal-autoscroll');
    if (btn) {
      if (this.isAutoScroll) {
        btn.textContent = "Pause";
      } else {
        btn.textContent = "Resume";
      }
    }
  }

  toggleAutoScroll() {
    this.isAutoScroll = !this.isAutoScroll;
    if (this.isAutoScroll) {
      this.container.scrollTop = this.container.scrollHeight;
    }
    this.updatePauseButton();
  }

  setFilter(query) {
    this.filterQuery = query.toLowerCase();
    this.filteredIndices = [];
    if (this.filterQuery) {
      for (let i = 0; i < this.lines.length; i++) {
        if (stripAnsi(this.lines[i]).toLowerCase().includes(this.filterQuery)) {
          this.filteredIndices.push(i);
        }
      }
    }

    for (const [index, node] of this.visibleNodes.entries()) {
      node.remove();
    }
    this.visibleNodes.clear();

    const displayedCount = this.filterQuery ? this.filteredIndices.length : this.lines.length;
    this.spacer.style.height = `${(displayedCount * this.lineHeight) + 40}px`;

    if (this.isAutoScroll) {
      this.container.scrollTop = this.container.scrollHeight;
    }
    this.render();
  }

  append(text) {
    this.buffer += text;
    let newlineIdx;
    let added = false;
    
    while ((newlineIdx = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.substring(0, newlineIdx);
      this.lines.push(line);

      if (this.filterQuery) {
         if (stripAnsi(line).toLowerCase().includes(this.filterQuery)) {
           this.filteredIndices.push(this.lines.length - 1);
         }
      }

      this.buffer = this.buffer.substring(newlineIdx + 1);
      added = true;
    }
    
    if (this.lines.length > this.maxLines) {
      const overflow = this.lines.length - this.maxLines;
      this.lines.splice(0, overflow);

      let filteredOverflow = 0;
      if (this.filterQuery) {
        this.filteredIndices = this.filteredIndices
          .map(i => {
              const newI = i - overflow;
              if (newI < 0) filteredOverflow++;
              return newI;
          })
          .filter(i => i >= 0);
      }

      const shiftAmount = this.filterQuery ? filteredOverflow : overflow;
      const newVisibleNodes = new Map();
      for (const [key, node] of this.visibleNodes.entries()) {
        const newKey = key - shiftAmount;
        if (newKey >= 0) {
          node.style.top = `${newKey * this.lineHeight}px`;
          newVisibleNodes.set(newKey, node);
        } else {
          node.remove();
        }
      }
      this.visibleNodes = newVisibleNodes;
    }
    
    if (added) {
      const displayedCount = this.filterQuery ? this.filteredIndices.length : this.lines.length;
      this.spacer.style.height = `${(displayedCount * this.lineHeight) + 40}px`;
      if (this.isAutoScroll) {
        this.container.scrollTop = this.container.scrollHeight;
      }
      this.render();
    }
  }

  render() {
    const scrollTop = this.container.scrollTop;
    const viewportHeight = this.container.clientHeight || 320;
    
    const displayedCount = this.filterQuery ? this.filteredIndices.length : this.lines.length;

    const startIndex = Math.max(0, Math.floor(scrollTop / this.lineHeight) - 5);
    const endIndex = Math.min(displayedCount - 1, startIndex + Math.ceil(viewportHeight / this.lineHeight) + 10);
    
    const sel = window.getSelection();
    const hasSelection = sel && sel.rangeCount > 0 && !sel.isCollapsed;

    // Remove nodes that are out of bounds and NOT selected
    for (const [index, node] of this.visibleNodes.entries()) {
      if (index < startIndex || index > endIndex) {
        if (hasSelection && sel.containsNode(node, true)) {
          continue; // keep for selection integrity
        }
        node.remove();
        this.visibleNodes.delete(index);
      }
    }
    
    // Add missing visible nodes in DOM order
    let currentChild = this.content.firstChild;
    // Collect all indices we need to ensure are present (both selected out-of-bounds and currently visible in-bounds)
    const neededIndices = Array.from(this.visibleNodes.keys());
    for (let i = startIndex; i <= endIndex; i++) {
      if (!neededIndices.includes(i)) neededIndices.push(i);
    }
    neededIndices.sort((a, b) => a - b);
    
    for (const i of neededIndices) {
      if (i < 0 || i >= displayedCount) continue;

      let node = this.visibleNodes.get(i);
      if (!node) {
        const lineIndex = this.filterQuery ? this.filteredIndices[i] : i;
        const rawLine = this.lines[lineIndex] === '' ? ' ' : this.lines[lineIndex];

        node = document.createElement('div');
        node.innerHTML = parseAnsiToHtml(rawLine);
        node.className = 'log-line';
        node.style.fontFamily = "'Geist Mono', monospace";
        node.style.fontSize = "0.8125rem";
        node.style.lineHeight = "1.5";
        node.style.color = "#a7f3d0";
        node.style.position = "absolute";
        node.style.top = `${i * this.lineHeight}px`;
        node.style.left = "1.25rem";
        node.style.right = "1.25rem";
        node.style.whiteSpace = "pre-wrap";
        node.style.wordBreak = "break-all";
        
        this.visibleNodes.set(i, node);
      }
      
      // Ensure DOM order
      if (node.parentNode !== this.content) {
        if (currentChild) {
          this.content.insertBefore(node, currentChild);
        } else {
          this.content.appendChild(node);
        }
      }
      currentChild = node.nextSibling;
    }
  }

  clear() {
    this.lines = [];
    this.filteredIndices = [];
    this.buffer = "";
    this.spacer.style.height = '40px';
    this.content.innerHTML = '';
    this.visibleNodes.clear();
  }

  getText() {
    return stripAnsi(this.lines.join('\n') + (this.buffer ? '\n' + this.buffer : ''));
  }
}

let virtualViewer = null;
let liveBacklogText  = "";

let initialOffset    = 0;
let lastSavedOffset  = 0;

function checkResumeState() {
  try {
    const saved = localStorage.getItem('beam_resume');
    if (saved) {
      const data = JSON.parse(saved);
      if (data.name === currentFile.name && data.size === currentFile.size) {
        if (confirm(`Resume partial download of ${currentFile.name} from ${formatBytes(data.offset)}?`)) {
          initialOffset = data.offset;
          receivedBytes = initialOffset;
          lastSavedOffset = initialOffset;
          return;
        }
      }
    }
  } catch (e) {}
  localStorage.removeItem('beam_resume');
  initialOffset = 0;
  lastSavedOffset = 0;
}

function maybeSaveProgress() {
  if (!currentFile || currentFile.size === -1) return;
  if (receivedBytes - lastSavedOffset >= 1024 * 1024) {
    localStorage.setItem('beam_resume', JSON.stringify({
      name: currentFile.name,
      size: currentFile.size,
      offset: receivedBytes
    }));
    lastSavedOffset = receivedBytes;
  }
}

// Direct-to-disk states
let diskWritableStream = null;
let diskFileHandle     = null;
let webrtcDataChannel  = null;
let currentShareURL    = "";
let useIndexedDB       = false;
let useOPFS            = false;

// ── OPFS Stream Writer (Safari / Firefox Fallback) ───────────────────────────
class OPFSStreamWriter {
  constructor(fileHandle, initialOffset = 0) {
    this.fileHandle = fileHandle;
    this.initialOffset = initialOffset;
    this.worker = null;
    this.offset = initialOffset;
  }

  async init() {
    if (typeof Worker === 'function') {
      const workerCode = `
        let fileHandle = null;
        let accessHandle = null;

        self.onmessage = async (e) => {
          const { type, chunk, initialOffset, handle, offset: msgOffset } = e.data;
          try {
            if (type === 'INIT') {
              fileHandle = handle;
              if (fileHandle && typeof fileHandle.createSyncAccessHandle === 'function') {
                accessHandle = await fileHandle.createSyncAccessHandle();
              } else {
                const root = await navigator.storage.getDirectory();
                const fh = await root.getFileHandle('beam_temp', { create: true });
                accessHandle = await fh.createSyncAccessHandle();
              }
              if (initialOffset === 0 && accessHandle && typeof accessHandle.truncate === 'function') {
                accessHandle.truncate(0);
              }
              self.postMessage({ type: 'INIT_OK' });
            } else if (type === 'WRITE') {
              if (!accessHandle) throw new Error("SyncAccessHandle not initialized");
              const view = new Uint8Array(chunk);
              let written = 0;
              if (typeof accessHandle.write === 'function') {
                written = accessHandle.write(view, { at: msgOffset });
              }
              self.postMessage({ type: 'WRITE_OK', written });
            } else if (type === 'CLOSE') {
              if (accessHandle) {
                if (typeof accessHandle.flush === 'function') accessHandle.flush();
                if (typeof accessHandle.close === 'function') accessHandle.close();
                accessHandle = null;
              }
              self.postMessage({ type: 'CLOSE_OK' });
            }
          } catch (err) {
            self.postMessage({ type: 'ERROR', error: err.message || String(err) });
          }
        };
      `;
      const blob = new Blob([workerCode], { type: 'application/javascript' });
      const url = URL.createObjectURL(blob);
      this.worker = new Worker(url);
      URL.revokeObjectURL(url);

      return new Promise((resolve, reject) => {
        const handleMsg = (e) => {
          if (e.data && e.data.type === 'INIT_OK') {
            this.worker.removeEventListener('message', handleMsg);
            resolve();
          } else if (e.data && e.data.type === 'ERROR') {
            this.worker.removeEventListener('message', handleMsg);
            this.worker.terminate();
            this.worker = null;
            reject(new Error(e.data.error));
          }
        };
        this.worker.addEventListener('message', handleMsg);
        try {
          this.worker.postMessage({ type: 'INIT', initialOffset: this.initialOffset, handle: this.fileHandle });
        } catch (_) {
          this.worker.postMessage({ type: 'INIT', initialOffset: this.initialOffset });
        }
      });
    } else {
      throw new Error("Web Worker API not available for OPFS SyncAccessHandle");
    }
  }

  async write(chunk) {
    if (!this.worker) throw new Error("OPFSStreamWriter not initialized");
    const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    const currOffset = this.offset;
    this.offset += bytes.byteLength;

    return new Promise((resolve, reject) => {
      const handleMsg = (e) => {
        if (e.data && e.data.type === 'WRITE_OK') {
          this.worker.removeEventListener('message', handleMsg);
          resolve();
        } else if (e.data && e.data.type === 'ERROR') {
          this.worker.removeEventListener('message', handleMsg);
          reject(new Error(e.data.error));
        }
      };
      this.worker.addEventListener('message', handleMsg);
      try {
        this.worker.postMessage({ type: 'WRITE', chunk: buffer, offset: currOffset }, [buffer]);
      } catch (_) {
        this.worker.postMessage({ type: 'WRITE', chunk: buffer, offset: currOffset });
      }
    });
  }

  async close() {
    if (!this.worker) return;
    return new Promise((resolve) => {
      const handleMsg = (e) => {
        if (e.data && e.data.type === 'CLOSE_OK') {
          if (this.worker) {
            this.worker.removeEventListener('message', handleMsg);
            this.worker.terminate();
            this.worker = null;
          }
          resolve();
        }
      };
      this.worker.addEventListener('message', handleMsg);
      this.worker.postMessage({ type: 'CLOSE' });
    });
  }
}

async function createOPFSWriter(fileHandle, initialOffset = 0) {
  if (fileHandle && typeof fileHandle.createWritable === 'function') {
    try {
      const writable = await fileHandle.createWritable({ keepExistingData: initialOffset > 0 });
      if (initialOffset > 0 && typeof writable.seek === 'function') {
        await writable.seek(initialOffset);
      } else if (initialOffset === 0 && typeof writable.truncate === 'function') {
        await writable.truncate(0);
      }
      return writable;
    } catch (e) {
      console.warn("createWritable failed on OPFS handle, trying worker fallback:", e);
    }
  }

  // Fallback: OPFSStreamWriter using Web Worker + createSyncAccessHandle
  const opfsWriter = new OPFSStreamWriter(fileHandle, initialOffset);
  await opfsWriter.init();
  return opfsWriter;
}

// ── Sequential Chunk Queue & Flow Control ─────────────────────────────────────
class SequentialChunkQueue {
  constructor({
    highWatermark = 16 * 1024 * 1024, // 16 MB
    lowWatermark = 4 * 1024 * 1024,   // 4 MB
    writeHandler,
    dataChannel = null,
    onError = null,
  } = {}) {
    this.queue = [];
    this.totalBytes = 0;
    this.highWatermark = highWatermark;
    this.lowWatermark = lowWatermark;
    this.writeHandler = writeHandler;
    this.dataChannel = dataChannel;
    this.onError = onError;

    this.isProcessing = false;
    this.isPaused = false;
    this.isEOF = false;
    this.isDrained = false;
    this.error = null;

    this.drainPromise = new Promise((resolve, reject) => {
      this._resolveDrain = resolve;
      this._rejectDrain = reject;
    });
    this.drainPromise.catch(() => {});
  }

  enqueue(chunk) {
    if (this.error || this.isEOF) return;

    const len = chunk.byteLength || chunk.length || 0;
    this.queue.push(chunk);
    this.totalBytes += len;

    if (this.totalBytes >= this.highWatermark && !this.isPaused) {
      this.isPaused = true;
      if (this.dataChannel && this.dataChannel.readyState === 'open') {
        try {
          this.dataChannel.send('PAUSE');
        } catch (e) {
          console.warn('Failed to send PAUSE signal:', e);
        }
      }
    }

    this._startProcessing();
  }

  enqueueEOF() {
    if (this.error) return;
    this.isEOF = true;
    this._startProcessing();
  }

  drain() {
    if (this.queue.length === 0 && this.isEOF && !this.isProcessing && !this.isDrained && !this.error) {
      this.isDrained = true;
      this._resolveDrain();
    }
    return this.drainPromise;
  }

  _startProcessing() {
    if (this.isProcessing) return;
    this.isProcessing = true;
    this._processLoop();
  }

  async _processLoop() {
    while (this.queue.length > 0) {
      if (this.error) break;

      const chunk = this.queue.shift();
      const len = chunk.byteLength || chunk.length || 0;

      try {
        await this.writeHandler(chunk);
        this.totalBytes -= len;

        if (this.isPaused && this.totalBytes <= this.lowWatermark) {
          this.isPaused = false;
          if (this.dataChannel && this.dataChannel.readyState === 'open') {
            try {
              this.dataChannel.send('RESUME');
            } catch (e) {
              console.warn('Failed to send RESUME signal:', e);
            }
          }
        }
      } catch (err) {
        this.error = err;
        this._rejectDrain(err);
        if (this.onError) this.onError(err);
        this.isProcessing = false;
        return;
      }
    }

    this.isProcessing = false;

    if (this.queue.length > 0 && !this.error) {
      this._startProcessing();
    } else if (this.queue.length === 0 && this.isEOF && !this.isDrained && !this.error) {
      this.isDrained = true;
      this._resolveDrain();
    }
  }
}

// ── WebRTC Stream Decrypter ────────────────────────────────────────────────────
class WebRTCStreamDecrypter {
  constructor(keyOrOptions, onChunk, onError) {
    if (keyOrOptions && typeof keyOrOptions === 'object' && !('algorithm' in keyOrOptions)) {
      this.key = keyOrOptions.key || null;
      this.onChunk = keyOrOptions.onChunk || null;
      this.onError = keyOrOptions.onError || null;
    } else {
      this.key = keyOrOptions || null;
      this.onChunk = onChunk || null;
      this.onError = onError || null;
    }

    this.buffer = new Uint8Array(0);
    this.isProcessing = false;
    this.hasError = false;
  }

  write(chunk) {
    if (this.hasError || !chunk) return;

    let data;
    if (chunk instanceof Uint8Array) {
      data = chunk;
    } else if (chunk instanceof ArrayBuffer) {
      data = new Uint8Array(chunk);
    } else if (ArrayBuffer.isView(chunk)) {
      data = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    } else {
      data = new Uint8Array(chunk);
    }

    if (!this.key) {
      if (this.onChunk) {
        try {
          this.onChunk(data);
        } catch (err) {
          this.hasError = true;
          if (this.onError) this.onError(err);
        }
      }
      return;
    }

    const newBuf = new Uint8Array(this.buffer.length + data.length);
    newBuf.set(this.buffer, 0);
    newBuf.set(data, this.buffer.length);
    this.buffer = newBuf;

    this._startProcessing();
  }

  push(chunk) { return this.write(chunk); }
  processChunk(chunk) { return this.write(chunk); }
  enqueue(chunk) { return this.write(chunk); }

  _startProcessing() {
    if (this.isProcessing || this.hasError) return;
    this.isProcessing = true;
    this._processLoop();
  }

  async _processLoop() {
    while (this.buffer.length >= 4) {
      if (this.hasError) break;

      const dv = new DataView(this.buffer.buffer, this.buffer.byteOffset, this.buffer.byteLength);
      const frameLen = dv.getUint32(0, false);

      if (frameLen < 12) {
        const err = new Error(`Invalid AES-GCM frame length header: ${frameLen} bytes (minimum 12 bytes required)`);
        this.hasError = true;
        this.buffer = new Uint8Array(0);
        if (this.onError) this.onError(err);
        break;
      }

      if (this.buffer.length < 4 + frameLen) {
        // Incomplete frame, wait for more data
        break;
      }

      const frame = this.buffer.subarray(4, 4 + frameLen);
      this.buffer = this.buffer.slice(4 + frameLen);

      const nonce = new Uint8Array(frame.subarray(0, 12));
      const ciphertext = new Uint8Array(frame.subarray(12));

      let decryptedBuffer;
      try {
        decryptedBuffer = await crypto.subtle.decrypt(
          { name: "AES-GCM", iv: nonce },
          this.key,
          ciphertext
        );
      } catch (err) {
        this.hasError = true;
        this.buffer = new Uint8Array(0);
        if (this.onError) this.onError(err);
        break;
      }

      const plaintext = new Uint8Array(decryptedBuffer);
      if (this.onChunk) {
        try {
          await this.onChunk(plaintext);
        } catch (err) {
          this.hasError = true;
          this.buffer = new Uint8Array(0);
          if (this.onError) this.onError(err);
          break;
        }
      }
    }

    this.isProcessing = false;
  }
}

// ── IndexedDB Buffering ────────────────────────────────────────────────────────
const IDB_NAME = 'GazeTransferDB';
const IDB_STORE = 'chunks';
let idb = null;
let idbChunkIndex = 0;

function initIDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (db.objectStoreNames.contains(IDB_STORE)) {
        db.deleteObjectStore(IDB_STORE);
      }
      db.createObjectStore(IDB_STORE);
    };
    req.onsuccess = (e) => {
      idb = e.target.result;
      const tx = idb.transaction(IDB_STORE, 'readonly');
      const countReq = tx.objectStore(IDB_STORE).count();
      countReq.onsuccess = (e2) => {
        idbChunkIndex = e2.target.result;
        resolve();
      };
      countReq.onerror = () => resolve();
    };
    req.onerror = (e) => reject(e.target.error);
  });
}

async function clearIDB() {
  if (!idb) await initIDB();
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readwrite');
    tx.objectStore(IDB_STORE).clear();
    tx.oncomplete = () => {
      idbChunkIndex = 0;
      resolve();
    };
    tx.onerror = (e) => reject(e.target.error);
  });
}

function storeChunkIDB(chunk) {
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readwrite');
    const index = idbChunkIndex++;
    tx.objectStore(IDB_STORE).put(chunk, index);
    tx.oncomplete = () => resolve();
    tx.onerror = (e) => reject(e.target.error);
  });
}

function getAllChunksIDB(mimeType = 'application/octet-stream') {
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readonly');
    const req = tx.objectStore(IDB_STORE).openCursor();
    const blobs = [];
    let currentBatch = [];
    let currentBatchSize = 0;
    const BATCH_LIMIT = 10 * 1024 * 1024; // 10 MB

    req.onsuccess = (e) => {
      const cursor = e.target.result;
      if (cursor) {
        currentBatch.push(cursor.value);
        currentBatchSize += cursor.value.byteLength || cursor.value.size || cursor.value.length || 0;

        if (currentBatchSize >= BATCH_LIMIT) {
          blobs.push(new Blob(currentBatch));
          currentBatch = [];
          currentBatchSize = 0;
        }
        cursor.continue();
      } else {
        if (currentBatch.length > 0) {
          blobs.push(new Blob(currentBatch));
        }
        resolve(new Blob(blobs, { type: mimeType }));
      }
    };
    req.onerror = (e) => reject(e.target.error);
  });
}

// ── State machine ─────────────────────────────────────────────────────────────
const STATES = ['receive-home', 'loading', 'ready', 'webrtc', 'downloading', 'livepipe', 'done', 'error', 'send-home', 'send-ready', 'send-sharing'];

function setState(name) {
  STATES.forEach((s) => {
    const el = document.getElementById(`state-${s}`);
    if (el) el.classList.toggle('hidden', s !== name);
  });
  // Re-trigger entry animation
  const el = document.getElementById(`state-${name}`);
  if (el) { el.style.animation = 'none'; void el.offsetWidth; el.style.animation = ''; }
}

function setLoadingSub(text) {
  const el = document.getElementById('loading-sub');
  if (el) el.textContent = text;
}

function setWebRTCSub(text) {
  const el = document.getElementById('webrtc-sub');
  if (el) el.textContent = text;
}

function markStep(id, done = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('step-done', done);
  el.classList.toggle('step-active', !done);
}

function setMode(mode, label) {
  transferMode = mode;
  const dot   = document.getElementById('mode-dot');
  const lbl   = document.getElementById('mode-label');
  const foot  = document.getElementById('transfer-mode-footer');
  if (dot)  dot.className  = `mode-dot mode-${mode}`;
  if (lbl)  lbl.textContent = label;
  if (foot) foot.textContent = label;
}

// ── Service Worker Pipe ───────────────────────────────────────────────────────
async function getSWPipe(fileMeta) {
  if (!('serviceWorker' in navigator) || !navigator.serviceWorker.controller) return null;

  // WebKit (Safari / Mobile Safari) does not reliably route iframe navigations through SW fetch handlers
  const isWebKit = typeof navigator !== 'undefined' && (/AppleWebKit/i.test(navigator.userAgent) && !/Chrome|Chromium|Edg|Firefox/i.test(navigator.userAgent));
  if (isWebKit) {
    console.warn("Service Worker iframe pipe not supported in WebKit; bypassing SW pipe.");
    return null;
  }

  try {
    let cancelTimeout;
    const swReady = navigator.serviceWorker.ready;
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('SW ready timeout')), 10000));
    const reg = await Promise.race([swReady, timeout]);

    if (!navigator.serviceWorker.controller) {
      await new Promise((resolve) => {
        if (navigator.serviceWorker.controller) {
          resolve();
          return;
        }
        const onControllerChange = () => {
          navigator.serviceWorker.removeEventListener('controllerchange', onControllerChange);
          resolve();
        };
        navigator.serviceWorker.addEventListener('controllerchange', onControllerChange);
        setTimeout(resolve, 500);
      });
    }

    const sw = navigator.serviceWorker.controller;
    if (!sw) {
      console.warn("Page is not controlled by a Service Worker, falling back to storage/RAM");
      return null;
    }

    const swUrl = `/sw-download-pipe/${Math.random().toString(36).substring(2)}`;
    const channel = new MessageChannel();
    const port = channel.port1;

    const readyPromise = new Promise((resolve, reject) => {
      const readyTimeout = setTimeout(() => {
        cleanup();
        reject(new Error('SW confirmation timeout'));
      }, 10000);

      function onMessage(e) {
        if (e.data && (e.data.type === 'READY' || e.data.type === 'PORT_READY' || e.data.type === 'INIT_PORT_ACK')) {
          cleanup();
          resolve();
        }
      }

      function cleanup() {
        clearTimeout(readyTimeout);
        port.removeEventListener('message', onMessage);
      }

      port.addEventListener('message', onMessage);
      port.start();
    });

    sw.postMessage({
      type: 'INIT_PORT',
      url: swUrl,
      filename: fileMeta.name,
      size: fileMeta.size,
      mime: fileMeta.mime
    }, [channel.port2]);

    await readyPromise;

    const iframe = document.createElement('iframe');
    iframe.hidden = true;
    document.body.appendChild(iframe);
    iframe.src = swUrl;

    setTimeout(() => {
      try { iframe.remove(); } catch (_) {}
    }, 10000);

    return port;
  } catch (err) {
    console.warn("Failed to get SW pipe, falling back to storage/RAM:", err);
    return null;
  }
}

const RAM_WARNING_THRESHOLD = 500 * 1024 * 1024; // 500 MB

function checkRamWarning(size) {
  return new Promise((resolve) => {
    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    const opfsSupported = !!(navigator.storage && navigator.storage.getDirectory);
    const swSupported = 'serviceWorker' in navigator;
    // We shouldn't warn if disk stream, OPFS stream, service worker stream, or IndexedDB are used.
    // Wait, IndexedDB buffering is always used as a fallback now. So maybe we don't need warning?
    // Actually, IndexedDB is used, but we'll stick to the original logic or improve it:
    // If we have swSupported or opfsSupported, we don't need to warn.
    if (useDiskStream || opfsSupported || swSupported || size <= RAM_WARNING_THRESHOLD || size === -1) {
      resolve(true);
      return;
    }
    
    const modal = document.getElementById('ram-warning-modal');
    if (!modal) {
      resolve(true);
      return;
    }
    
    modal.classList.remove('hidden');
    
    const abortBtn = document.getElementById('btn-ram-abort');
    const proceedBtn = document.getElementById('btn-ram-proceed');
    
    const cleanup = () => {
      modal.classList.add('hidden');
      abortBtn.removeEventListener('click', onAbort);
      proceedBtn.removeEventListener('click', onProceed);
    };
    
    const onAbort = () => { cleanup(); resolve(false); };
    const onProceed = () => { cleanup(); resolve(true); };
    
    abortBtn.addEventListener('click', onAbort);
    proceedBtn.addEventListener('click', onProceed);
  });
}

function generateClientQRCodeDataURL(text) {
  if (typeof generateQRCodeDataURL === 'function') {
    return generateQRCodeDataURL(text);
  }
  if (typeof generateQRCodeSVGDataURL === 'function') {
    return generateQRCodeSVGDataURL(text);
  }
  if (typeof window !== 'undefined' && window.qrcode && typeof window.qrcode.generateQRCodeSVGDataURL === 'function') {
    return window.qrcode.generateQRCodeSVGDataURL(text);
  }
  return '';
}

// ── QR Scanning ───────────────────────────────────────────────────────────────
let qrStream = null;
let qrScanFrame = null;
let barcodeDetector = null;

async function startQRScanner() {
  const modal = document.getElementById('qr-scan-modal');
  const video = document.getElementById('qr-video');
  const errorDiv = document.getElementById('qr-scan-error');

  if (!modal || !video || !errorDiv) return;

  modal.classList.remove('hidden');
  errorDiv.classList.add('hidden');

  if (!('BarcodeDetector' in window)) {
    errorDiv.textContent = "QR scanning is not supported by your browser.";
    errorDiv.classList.remove('hidden');
    return;
  }

  if (!barcodeDetector) {
    try {
        barcodeDetector = new window.BarcodeDetector({ formats: ['qr_code'] });
    } catch(err) {
        errorDiv.textContent = "QR scanning is not supported by your browser.";
        errorDiv.classList.remove('hidden');
        return;
    }
  }

  try {
    qrStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } });
    video.srcObject = qrStream;

    video.onloadedmetadata = () => {
      video.play();
      scanQRCode();
    };
  } catch (err) {
    console.error("Camera error:", err);
    errorDiv.textContent = "Camera access denied or unavailable.";
    errorDiv.classList.remove('hidden');
  }
}

function stopQRScanner() {
  const modal = document.getElementById('qr-scan-modal');
  if (modal) modal.classList.add('hidden');

  if (qrScanFrame) {
    cancelAnimationFrame(qrScanFrame);
    qrScanFrame = null;
  }

  if (qrStream) {
    qrStream.getTracks().forEach(track => track.stop());
    qrStream = null;
  }

  const video = document.getElementById('qr-video');
  if (video) video.srcObject = null;
}

async function scanQRCode() {
  const video = document.getElementById('qr-video');
  if (!video || !qrStream) return;

  try {
    const barcodes = await barcodeDetector.detect(video);
    if (barcodes.length > 0) {
      const url = barcodes[0].rawValue;
      stopQRScanner();
      window.location.href = url;
      return;
    }
  } catch (err) {
    // Ignore frame errors, continue scanning
  }

  qrScanFrame = requestAnimationFrame(scanQRCode);
}

function parseSessionInput(input) {
  if (!input) return null;
  input = input.trim();
  if (!input) return null;

  if (input.startsWith('http://') || input.startsWith('https://')) {
    try {
      return new URL(input).href;
    } catch (e) {
      return null;
    }
  }

  const baseOrigin = (typeof window !== 'undefined' && window.location && window.location.origin) 
    ? window.location.origin 
    : 'https://beamshare.app';

  if (input.startsWith('/') || input.startsWith('?') || input.startsWith('#')) {
    return baseOrigin + (input.startsWith('/') ? input : '/' + input);
  }

  const url = new URL(baseOrigin);
  url.searchParams.set('s', input);
  return url.href;
}

function handleJoinSession(e) {
  if (e && e.preventDefault) e.preventDefault();
  const inputEl = document.getElementById('input-session-code');
  if (!inputEl) return;
  const targetUrl = parseSessionInput(inputEl.value);
  if (targetUrl) {
    window.location.href = targetUrl;
  } else {
    showError("Please enter a valid session ID, transfer link, or passphrase.");
  }
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
function init() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js', { scope: '/' }).catch(err => {
      console.warn('Service Worker registration failed:', err);
    });

    const markSwReady = () => {
      document.documentElement.setAttribute('data-sw-ready', 'true');
    };

    if (navigator.serviceWorker.controller) {
      markSwReady();
    } else if (typeof navigator.serviceWorker.addEventListener === 'function') {
      navigator.serviceWorker.addEventListener('controllerchange', markSwReady, { once: true });
    }

    navigator.serviceWorker.ready.then(() => {
      markSwReady();
    }).catch(() => {});
  }

  setState('loading');
  setMode('connecting', 'Connecting…');

  document.getElementById('btn-download')?.addEventListener('click', startHTTPDownload);
  document.getElementById('btn-upload-trigger')?.addEventListener('click', () => {
    document.getElementById('file-upload-input')?.click();
  });
  document.getElementById('file-upload-input')?.addEventListener('change', handleUploadFile);
  document.getElementById('btn-retry')?.addEventListener('click', () => {
    resetState();
    setTimeout(bootstrap, 400);
  });
  document.getElementById('btn-again')?.addEventListener('click', () => {
    window.location.href = window.location.pathname; // strip query params to go to receive-home
  });

  document.getElementById('btn-scan-qr')?.addEventListener('click', startQRScanner);
  document.getElementById('btn-qr-cancel')?.addEventListener('click', stopQRScanner);
  document.getElementById('form-join-session')?.addEventListener('submit', handleJoinSession);

  document.getElementById('btn-copy-share-url')?.addEventListener('click', () => {
    if (currentShareURL) {
      navigator.clipboard.writeText(currentShareURL);
      const btn = document.getElementById('btn-copy-share-url');
      const old = btn.textContent;
      btn.textContent = "Copied URL!";
      setTimeout(() => btn.textContent = old, 1500);
    }
  });

  document.getElementById('btn-terminal-copy')?.addEventListener('click', () => {
    if (virtualViewer) {
      navigator.clipboard.writeText(virtualViewer.getText());
      const btn = document.getElementById('btn-terminal-copy');
      const old = btn.textContent;
      btn.textContent = "Copied!";
      setTimeout(() => btn.textContent = old, 1500);
    }
  });

  document.getElementById('terminal-search')?.addEventListener('input', (e) => {
    if (virtualViewer) {
      virtualViewer.setFilter(e.target.value);
    }
  });

  document.getElementById('btn-terminal-autoscroll')?.addEventListener('click', () => {
    if (virtualViewer) {
      virtualViewer.toggleAutoScroll();
    }
  });

  document.getElementById('btn-terminal-download')?.addEventListener('click', async () => {
    if (useIndexedDB) {
      const blob = await getAllChunksIDB('text/plain;charset=utf-8');
      triggerSave(blob, currentFile ? currentFile.name : 'stream.log');
    } else {
      if (virtualViewer) {
        const blob = new Blob([virtualViewer.getText()], { type: 'text/plain;charset=utf-8' });
        triggerSave(blob, currentFile ? currentFile.name : 'stream.log');
      }
    }
  });

  // Tab Switching Click Listeners
  const receiveBtn = document.getElementById('tab-receive');
  const sendBtn = document.getElementById('tab-send');
  
  receiveBtn?.addEventListener('click', () => switchTab('receive'));
  sendBtn?.addEventListener('click', () => switchTab('send'));

  // Sender File drop/select UI handlers
  const sendDropZone = document.getElementById('send-drop-zone');
  const senderFileInput = document.getElementById('sender-file-input');

  sendDropZone?.addEventListener('click', () => senderFileInput?.click());
  senderFileInput?.addEventListener('change', (e) => {
    handleSenderFileSelect(e.target.files[0]);
  });

  sendDropZone?.addEventListener('dragover', (e) => {
    e.preventDefault();
    sendDropZone.classList.add('dragover');
  });
  sendDropZone?.addEventListener('dragleave', () => {
    sendDropZone.classList.remove('dragover');
  });
  sendDropZone?.addEventListener('drop', (e) => {
    e.preventDefault();
    sendDropZone.classList.remove('dragover');
    handleSenderFileSelect(e.dataTransfer.files[0]);
  });

  // Sender button actions
  document.getElementById('btn-start-share')?.addEventListener('click', startSenderSharing);
  document.getElementById('btn-cancel-share')?.addEventListener('click', () => {
    senderFile = null;
    setState('send-home');
  });
  document.getElementById('btn-stop-share')?.addEventListener('click', stopSenderSharing);

  document.getElementById('btn-copy-send-url')?.addEventListener('click', () => {
    const input = document.getElementById('send-url-input');
    if (input) {
      input.select();
      navigator.clipboard.writeText(input.value);
      const btn = document.getElementById('btn-copy-send-url');
      const old = btn.textContent;
      btn.textContent = "Copied!";
      setTimeout(() => btn.textContent = old, 1500);
    }
  });

  initSpotlight();
  bootstrap();
}

function resetState() {
  currentFile = null;
  receivedChunks = [];
  receivedBytes = 0;
  isLivePipeMode = false;
  liveBacklogText = "";
  diskWritableStream = null;
  diskFileHandle = null;
  webrtcDataChannel = null;
  currentShareURL = "";
  useIndexedDB = false;
  useOPFS = false;
  navigator.storage?.getDirectory().then(root => root.removeEntry('beam_temp').catch(()=>{})).catch(()=>{});
  if (idb) clearIDB().catch(console.error);
  document.getElementById('done-share-container')?.classList.add('hidden');
  
  if (!virtualViewer) {
    virtualViewer = new VirtualLogViewer('.terminal-body', 100000);
  }
  if (virtualViewer) {
    virtualViewer.clear();
    virtualViewer.append("\x1b[90m// Waiting for stream input...\x1b[0m\n");
  }
}

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  const isWebRTCMode = params.get('mode') === 'webrtc' || params.get('sdp') || params.get('offer');
  let localURL = params.get('local');
  const sessionID = params.get('s');

  if (!localURL && !sessionID) {
    if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || /^(\d{1,3}\.){3}\d{1,3}$/.test(window.location.hostname)) {
      localURL = window.location.origin;
    }
  }

  // Reset error UI states in case they were previously set
  const errorLabel = document.querySelector('#state-error .state-label');
  if (errorLabel) errorLabel.textContent = "Connection error";
  const retryBtn = document.getElementById('btn-retry');
  if (retryBtn) retryBtn.classList.remove('hidden');

  const tabHeader = document.getElementById('tab-header');
  if (localURL || sessionID) {
    if (tabHeader) tabHeader.classList.add('hidden');
  } else {
    if (tabHeader) tabHeader.classList.remove('hidden');
  }

  if (!localURL && !sessionID) {
    setState('receive-home');
    setMode('ready', 'Gaze is ready');
    return;
  }

  if (localURL) {
    try {
      setLoadingSub('Attempting direct local connection…');
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 1500);
      
      const res = await fetch(`${localURL}/api/meta`, { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (res.ok) {
        console.log("Local connection successful, using as backend...");
        window.GAZE_BACKEND_URL = localURL;
        // Do not return, continue to bootstrap flow using localURL as backend
      }
    } catch (e) {
      console.warn("Direct local connection failed, falling back to relay:", e);
    }
  }

  if (isWebRTCMode && typeof RTCPeerConnection !== 'undefined') {
    setLoadingSub('Connecting WebRTC signaling tunnel…');
    try {
      await startWebRTC();
      return;
    } catch (err) {
      console.warn('WebRTC failed, falling back to HTTP:', err);
    }
  }

  // Fallback: regular HTTP mode
  setLoadingSub('Fetching file metadata…');
  await fetchMetaAndShowReady();
}

// ── HTTP mode ─────────────────────────────────────────────────────────────────
async function fetchMetaAndShowReady() {
  try {
    const res = await fetch(apiPath('/api/meta'));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    currentFile = await res.json();

    if (currentFile.size === -1) {
      // Live Pipe mode
      isLivePipeMode = true;
      setMode('http', 'Live Log (HTTP SSE)');
      startHTTPSSE();
    } else {
      checkResumeState();
      renderFileCard(currentFile);
      setMode('http', 'Direct LAN transfer');
      setState('ready');
    }
  } catch (err) {
    showError(`Could not reach the sender: ${err.message}`);
  }
}

function extractKeyFragment(hash) {
  if (!hash) return null;
  let rawHash = hash.startsWith('#') ? hash.slice(1) : hash;
  
  let decoded = rawHash;
  for (let i = 0; i < 3; i++) {
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    } catch (e) {
      break;
    }
  }

  let match = decoded.match(/(?:^|[&?#;])k=([^&;#\s]+)/i);
  if (!match) {
    match = rawHash.match(/(?:^|[&?#;])k(?:=|%3d|%3D)([^&;#\s]+)/i);
  }
  if (!match) return null;

  let keyStr = match[1];
  try {
    keyStr = decodeURIComponent(keyStr);
  } catch (e) {}

  keyStr = keyStr.replace(/-/g, '+').replace(/_/g, '/').replace(/\s/g, '');
  while (keyStr.length % 4 !== 0) {
    keyStr += '=';
  }
  return keyStr;
}

async function parseDecryptionKeyFromHash(hash) {
  const b64 = extractKeyFragment(hash);
  if (!b64) return null;
  const raw = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
  if (raw.length !== 32) {
    throw new Error(`Invalid decryption key length: expected 32 bytes, got ${raw.length}`);
  }
  return await crypto.subtle.importKey(
    "raw", raw, { name: "AES-GCM" }, false, ["decrypt"]
  );
}

async function startHTTPDownload() {
  if (!currentFile) return;

  const proceed = await checkRamWarning(currentFile.size);
  if (!proceed) {
    return;
  }

  startTime     = Date.now();
  receivedBytes = 0;
  totalBytes    = currentFile.size;

  // Try to use FileSystem API for streaming to disk if supported
  const useDiskStream = typeof window.showSaveFilePicker === 'function';
  const opfsSupported = !!(navigator.storage && navigator.storage.getDirectory);
  const swSupported = 'serviceWorker' in navigator;
  useIndexedDB = !useDiskStream && !opfsSupported && !swSupported;

  if (!useDiskStream && !opfsSupported && !swSupported) {
    console.warn("Advanced streaming not supported. Large files may cause Out of Memory errors.");
    alert("Warning: Streaming direct-to-disk is not supported in this browser. Large files may fail due to RAM limits.");
  }

  let swPipePort = null;
  if (useDiskStream) {
    try {
      diskFileHandle = await window.showSaveFilePicker({
        suggestedName: currentFile.name,
      });
      diskWritableStream = await diskFileHandle.createWritable();
      useIndexedDB = false;
    } catch (pickerErr) {
      console.warn("Direct-to-disk picker cancelled/failed, falling back to SW, OPFS, or IndexedDB:", pickerErr);
      diskWritableStream = null;
    }
  }

  if (!diskWritableStream && opfsSupported) {
    try {
      const root = await navigator.storage.getDirectory();
      const opfsFileName = `beam_temp_${Date.now()}_${Math.random().toString(36).substring(2)}`;
      
      const estimate = await navigator.storage.estimate();
      if (estimate && estimate.quota && currentFile.size > (estimate.quota - estimate.usage)) {
         throw new Error("Device disk is full");
      }
      
      diskFileHandle = await root.getFileHandle(opfsFileName, { create: true });
      diskWritableStream = await createOPFSWriter(diskFileHandle, initialOffset);
      useOPFS = true;
      useIndexedDB = false;
    } catch (err) {
      console.error("OPFS init failed:", err);
      if (err.message === "Device disk is full") {
         showError("Not enough disk space for transfer.");
         return;
      }
      diskWritableStream = null;
      useOPFS = false;
    }
  }

  if (!diskWritableStream && swSupported) {
    swPipePort = await getSWPipe(currentFile);
  }

  if (!diskWritableStream && !swPipePort && !useOPFS) {
    useIndexedDB = true;
    if (initialOffset === 0) {
      await clearIDB();
    }
    receivedChunks = [];
  }

  setState('downloading');
  updateProgress(initialOffset / totalBytes || 0);

  try {
    const headers = {};
    if (initialOffset > 0) {
      headers['Range'] = `bytes=${initialOffset}-`;
    }
    const res = await fetch(apiPath('/api/download'), { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const reader = res.body.getReader();
    let received = initialOffset;
    let encBuffer = new Uint8Array(0);

    while (true) {
      const { done, value } = await reader.read();

      if (value && value.length > 0) {
        if (decryptionKey) {
          let newBuffer = new Uint8Array(encBuffer.length + value.length);
          newBuffer.set(encBuffer, 0);
          newBuffer.set(value, encBuffer.length);
          encBuffer = newBuffer;

          while (encBuffer.length >= 4) {
            const dv = new DataView(encBuffer.buffer, encBuffer.byteOffset, encBuffer.byteLength);
            const frameLen = dv.getUint32(0, false);
            const maxFrameSize = 65536 + 12 + 16; // 64KB chunk + 12B nonce + 16B tag
            if (frameLen < 12 || frameLen > maxFrameSize) {
              console.error("Invalid encrypted frame size:", frameLen);
              encBuffer = new Uint8Array(0);
              throw new Error("Invalid encrypted frame size: " + frameLen);
            }
            if (encBuffer.length >= 4 + frameLen) {
              const frame = encBuffer.slice(4, 4 + frameLen);
              encBuffer = encBuffer.slice(4 + frameLen);
              
              const nonce = new Uint8Array(frame.subarray(0, 12));
              const ciphertext = new Uint8Array(frame.subarray(12));
              const decrypted = await crypto.subtle.decrypt(
                { name: "AES-GCM", iv: nonce },
                decryptionKey,
                ciphertext
              );
              const decValue = new Uint8Array(decrypted);
              
              if (diskWritableStream) {
                await diskWritableStream.write(decValue);
              } else if (swPipePort) {
                swPipePort.postMessage(decValue);
              } else if (useIndexedDB) {
                await storeChunkIDB(decValue);
              } else {
                receivedChunks.push(decValue);
              }
              received += decValue.length;
            } else {
              break;
            }
          }
        } else {
          if (diskWritableStream) {
            await diskWritableStream.write(value);
          } else if (swPipePort) {
            swPipePort.postMessage(value);
          } else if (useIndexedDB) {
            await storeChunkIDB(value);
          } else {
            receivedChunks.push(value);
          }
          received += value.length;
        }

        receivedBytes = received;
        maybeSaveProgress();
        if (totalBytes > 0) {
          updateProgress(received / totalBytes);
          updateDLStats(received, totalBytes);
          updateSpeed(received);
        }
      }

      if (done) break;
    }

    if (totalBytes > 0 && received < totalBytes) {
      throw new Error(`Connection closed prematurely. Received ${formatBytes(received)} of ${formatBytes(totalBytes)}.`);
    }

    if (diskWritableStream) {
      await diskWritableStream.close();
      if (useOPFS) {
        const file = await diskFileHandle.getFile();
        const buffer = await file.arrayBuffer();
        const blob = new Blob([buffer], { type: currentFile ? currentFile.mime : file.type });
        try { const root = await navigator.storage.getDirectory(); await root.removeEntry(diskFileHandle.name); } catch(e){}
        triggerSave(blob, currentFile.name);
      }
    } else if (swPipePort) {
      swPipePort.postMessage('EOF');
    } else {
      let finalBlob;
      if (useIndexedDB) {
        finalBlob = await getAllChunksIDB(currentFile.mime);
        await clearIDB();
      } else {
        finalBlob = new Blob(receivedChunks, { type: currentFile.mime });
      }
      triggerSave(finalBlob, currentFile.name);
    }

    let modeDesc = diskWritableStream ? (useOPFS ? 'LAN HTTP (OPFS)' : 'LAN HTTP (Direct Disk)') : (swPipePort ? 'LAN HTTP (SW Pipe)' : (useIndexedDB ? 'LAN HTTP (IndexedDB)' : 'LAN HTTP (RAM Blob)'));
    showDone(currentFile.name, currentFile.size, modeDesc);

  } catch (err) {
    if (err.name === 'QuotaExceededError' || err.message.includes('Quota') || (err.message && err.message.includes('disk is full'))) {
      showError("Transfer failed: Device disk is full.");
    } else {
      showError(`Download failed: ${err.message}`);
    }
  }
}

// ── HTTP SSE (Server-Sent Events) live log streamer ─────────────────────────
async function startHTTPSSE() {
  setState('livepipe');
  const source = new EventSource(apiPath('/api/live/stream'));
  if (virtualViewer) virtualViewer.clear();

  const useDiskStream = typeof window.showSaveFilePicker === 'function';
  useIndexedDB = !useDiskStream;
  if (useIndexedDB) {
    await clearIDB();
  }

  source.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "backlog" || msg.type === "data") {
        appendTerminalText(msg.payload);
        if (useIndexedDB) {
          storeChunkIDB(msg.payload);
        }
      } else if (msg.type === "eof") {
        source.close();
        // Change pulsing dot status
        const dot = document.querySelector('.live-pulse-dot');
        if (dot) {
          dot.style.background = 'var(--zinc-600)';
          dot.style.boxShadow = 'none';
          dot.style.animation = 'none';
        }
        showDone('Stream Log', receivedBytes, 'HTTP SSE Stream');
      }
    } catch (err) {
      console.error("SSE parse error:", err);
    }
  };

  source.onerror = () => {
    source.close();
    showError('SSE stream connection terminated.');
  };
}

// ── Decompression helper ──────────────────────────────────────────────────────
async function decompressOffer(base64Str) {
  // Decode URL-safe base64
  let b64 = base64Str.replace(/-/g, '+').replace(/_/g, '/');
  // Pad with '=' if necessary
  while (b64.length % 4 !== 0) {
    b64 += '=';
  }
  const binStr = atob(b64);
  const len = binStr.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = binStr.charCodeAt(i);
  }
  
  const decompressed = pako.inflate(bytes);
  return new TextDecoder().decode(decompressed);
}

// ── WebRTC P2P mode ───────────────────────────────────────────────────────────
async function startWebRTC() {
  setState('webrtc');
  setMode('webrtc', 'WebRTC P2P (optical handshake)');

  let decryptionKey = null;
  try {
    decryptionKey = await parseDecryptionKeyFromHash(window.location.hash);
  } catch (err) {
    console.error("Failed to import decryption key", err);
    showError("Decryption key error: " + err.message);
    return;
  }

  // 1. Fetch the full SDP offer from the server.
  let offer;
  const params = new URLSearchParams(window.location.search);
  const embeddedOffer = params.get('sdp') || params.get('offer');

  if (embeddedOffer) {
    setWebRTCSub('Decoding embedded SDP offer…');
    try {
      const decompressedSDP = await decompressOffer(embeddedOffer);
      offer = {
        type: 'offer',
        sdp: decompressedSDP,
        iceServers: STUN_SERVERS
      };
    } catch (err) {
      console.warn("Failed to decompress embedded offer, falling back to HTTP fetch", err);
    }
  }

  if (!offer) {
    setWebRTCSub('Fetching SDP offer…');
    const offerRes = await fetch(apiPath('/api/signal/offer'));
    if (!offerRes.ok) throw new Error(`offer fetch: HTTP ${offerRes.status}`);
    offer = await offerRes.json();
  }
  markStep('step-offer');

  // 2. Create peer connection and set remote description.
  const iceServers = params.get('no_stun') ? [] : getIceServers(offer);
  const pc = new RTCPeerConnection({ iceServers });

  // Also fetch file meta in parallel.
  const metaPromise = fetch(apiPath('/api/meta')).then(r => r.json());

  await pc.setRemoteDescription(new RTCSessionDescription(offer));

  // 3. Create answer.
  setWebRTCSub('Creating answer…');
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);

  setWebRTCSub('Gathering network candidates…');
  // Wait for ICE gathering.
  await new Promise((resolve) => {
    if (pc.iceGatheringState === 'complete') { resolve(); return; }
    pc.addEventListener('icegatheringstatechange', () => {
      if (pc.iceGatheringState === 'complete') resolve();
    });
    
    // Default to 10 seconds (10000 ms) unless overridden by the sender's offer payload/query param
    let waitTime = 10000;
    const urlTimeout = params.get('timeout');
    if (urlTimeout) {
      waitTime = parseInt(urlTimeout, 10);
    } else if (offer.timeout) {
      waitTime = offer.timeout;
    }
    if (waitTime > 60000) waitTime = 60000;
    setTimeout(resolve, waitTime);
  });

  // 4. POST answer to sender.
  setWebRTCSub('Sending answer to sender…');
  const answerRes = await fetch(apiPath('/api/signal/answer'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pc.localDescription),
  });
  if (!answerRes.ok) throw new Error(`answer POST: HTTP ${answerRes.status}`);
  markStep('step-answer');

  // 5. Fetch and add ICE candidates from sender.
  setWebRTCSub('Exchanging ICE candidates…');
  try {
    const candRes  = await fetch(apiPath('/api/signal/candidates'));
    const cands    = await candRes.json();
    for (const c of cands) {
      await pc.addIceCandidate(new RTCIceCandidate(c));
    }
  } catch { /* non-fatal — trickle ICE will handle it */ }
  markStep('step-ice');

  // 6. Wait for data channel from sender.
  setWebRTCSub('Waiting for data channel…');
  const dc = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('data channel timeout')), 30000);
    pc.ondatachannel = (e) => { clearTimeout(timer); resolve(e.channel); };
  });
  markStep('step-open');
  webrtcDataChannel = dc;

  // 7. Resolve file metadata.
  currentFile = await metaPromise;
  totalBytes  = currentFile.size;

  if (currentFile.size !== -1) {
    checkResumeState();
  }

  const proceed = await checkRamWarning(currentFile.size);
  if (!proceed) {
    if (webrtcDataChannel) webrtcDataChannel.close();
    pc.close();
    showError('Transfer aborted by user. File too large for memory.');
    return;
  }

  if (currentFile.size === -1) {
    // ── Live log stream over WebRTC data channel ─────────────────────────────
    isLivePipeMode = true;
    setState('livepipe');
    setMode('webrtc', 'WebRTC Live Log');
    if (virtualViewer) virtualViewer.clear();

    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    useIndexedDB = !useDiskStream;
    if (useIndexedDB) {
      clearIDB();
    }

    dc.onmessage = (e) => {
      if (typeof e.data === 'string') {
        if (e.data === "EOF") {
          // Finished
          const dot = document.querySelector('.live-pulse-dot');
          if (dot) {
            dot.style.background = 'var(--zinc-600)';
            dot.style.boxShadow = 'none';
            dot.style.animation = 'none';
          }
          showDone('Stream Log', receivedBytes, 'WebRTC Stream');
          pc.close();
        } else {
          appendTerminalText(e.data);
          if (useIndexedDB) {
            storeChunkIDB(e.data);
          }
        }
      }
    };
  } else {
    // ── Direct-to-Disk File transfer over WebRTC data channel ──────────────────
    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    const opfsSupported = !!(navigator.storage && navigator.storage.getDirectory);
    const swSupported = 'serviceWorker' in navigator;
    useIndexedDB = !useDiskStream && !opfsSupported && !swSupported;

    if (!useDiskStream && !opfsSupported && !swSupported) {
      console.warn("Advanced streaming not supported. Large files may cause Out of Memory errors.");
      alert("Warning: Streaming direct-to-disk is not supported in this browser. Large files may fail due to RAM limits.");
    }

    let swPipePort = null;
    if (useDiskStream) {
      try {
        diskFileHandle = await window.showSaveFilePicker({
          suggestedName: currentFile.name,
        });
        diskWritableStream = await diskFileHandle.createWritable({ keepExistingData: true });
        if (initialOffset > 0) {
          await diskWritableStream.seek(initialOffset);
        }
        useIndexedDB = false;
      } catch (pickerErr) {
        console.warn("WebRTC Direct-to-disk picker cancelled, falling back to SW, OPFS, or IndexedDB:", pickerErr);
        diskWritableStream = null;
      }
    }

    if (!diskWritableStream && opfsSupported) {
      try {
        const root = await navigator.storage.getDirectory();
        const opfsFileName = `beam_temp_${Date.now()}_${Math.random().toString(36).substring(2)}`;
        
        const estimate = await navigator.storage.estimate();
        if (estimate && estimate.quota && currentFile.size > (estimate.quota - estimate.usage)) {
           throw new Error("Device disk is full");
        }
        
        diskFileHandle = await root.getFileHandle(opfsFileName, { create: true });
        diskWritableStream = await createOPFSWriter(diskFileHandle, initialOffset);
        useOPFS = true;
        useIndexedDB = false;
      } catch (err) {
        console.error("OPFS init failed:", err);
        if (err.message === "Device disk is full") {
           showError("Not enough disk space for transfer.");
           pc.close();
           return;
        }
        diskWritableStream = null;
        useOPFS = false;
      }
    }

    if (!diskWritableStream && swSupported) {
      swPipePort = await getSWPipe(currentFile);
    }

    if (!diskWritableStream && !swPipePort && !useOPFS) {
      useIndexedDB = true;
      if (initialOffset === 0) {
        await clearIDB();
      }
      receivedChunks = [];
    }

    let decryptionKey = null;
    try {
      decryptionKey = await parseDecryptionKeyFromHash(window.location.hash);
    } catch(e) {
      console.error("Failed to import decryption key", e);
      showError("Decryption key error: " + e.message);
      if (webrtcDataChannel) webrtcDataChannel.close();
      pc.close();
      return;
    }

    setState('downloading');
    startTime     = Date.now();
    updateProgress(initialOffset / totalBytes || 0);

    await new Promise((resolve, reject) => {
      dc.binaryType = 'arraybuffer';
      if (dc.readyState === 'open') {
        dc.send(`OFFSET:${initialOffset}`);
      } else {
        dc.onopen = () => dc.send(`OFFSET:${initialOffset}`);
      }

      let encBuffer = new Uint8Array(0);

      const chunkQueue = new SequentialChunkQueue({
        highWatermark: 16 * 1024 * 1024,
        lowWatermark: 4 * 1024 * 1024,
        dataChannel: dc,
        onError: (err) => {
          if (err.name === 'QuotaExceededError' || err.code === 22 || (err.message && (err.message.includes('Quota') || err.message.includes('disk is full')))) {
            showError("Transfer failed: Device disk is full.");
          } else {
            showError(`Transfer failed: ${err.message}`);
          }
          try { dc.close(); } catch (e) {}
          reject(err);
        },
        writeHandler: async (chunk) => {
          if (decryptionKey) {
            let newBuffer = new Uint8Array(encBuffer.length + chunk.length);
            newBuffer.set(encBuffer, 0);
            newBuffer.set(chunk, encBuffer.length);
            encBuffer = newBuffer;

            while (encBuffer.length >= 4) {
              const dv = new DataView(encBuffer.buffer, encBuffer.byteOffset, encBuffer.byteLength);
              const frameLen = dv.getUint32(0, false);
              if (encBuffer.length >= 4 + frameLen) {
                const frame = encBuffer.slice(4, 4 + frameLen);
                encBuffer = encBuffer.slice(4 + frameLen);

                if (frame.length < 12) {
                  throw new Error("Invalid frame length: shorter than 12-byte nonce size");
                }

                const nonce = new Uint8Array(frame.subarray(0, 12));
                const ciphertext = new Uint8Array(frame.subarray(12));

                let decrypted;
                try {
                  decrypted = await crypto.subtle.decrypt(
                    { name: "AES-GCM", iv: nonce },
                    decryptionKey,
                    ciphertext
                  );
                } catch (decryptErr) {
                  throw new Error("Decryption failed: " + (decryptErr.message || String(decryptErr)));
                }

                const decValue = new Uint8Array(decrypted);

                if (diskWritableStream) {
                  await diskWritableStream.write(decValue);
                } else if (swPipePort) {
                  swPipePort.postMessage(decValue);
                } else if (useIndexedDB) {
                  await storeChunkIDB(decValue);
                } else {
                  receivedChunks.push(decValue);
                }

                receivedBytes += decValue.length;
                maybeSaveProgress();
                if (totalBytes > 0) {
                  updateProgress(receivedBytes / totalBytes);
                  updateDLStats(receivedBytes, totalBytes);
                  updateSpeed(receivedBytes);
                }
              } else {
                break;
              }
            }
          } else {
            if (diskWritableStream) {
              await diskWritableStream.write(chunk);
            } else if (swPipePort) {
              swPipePort.postMessage(chunk);
            } else if (useIndexedDB) {
              await storeChunkIDB(chunk);
            } else {
              receivedChunks.push(chunk);
            }

            receivedBytes += chunk.byteLength;
            maybeSaveProgress();
            if (totalBytes > 0) {
              updateProgress(receivedBytes / totalBytes);
              updateDLStats(receivedBytes, totalBytes);
              updateSpeed(receivedBytes);
            }
          }
        }
      });

      encBuffer = new Uint8Array(0);
      let decryptChain = Promise.resolve();

      dc.onmessage = (e) => {
        if (decryptionKey) {
          decryptChain = decryptChain.then(async () => {
            if (typeof e.data === 'string') {
              if (e.data === "EOF") {
                chunkQueue.enqueueEOF();
                try {
                  await chunkQueue.drain();
                  if (diskWritableStream) {
                    await diskWritableStream.close();
                    if (useOPFS) {
                      const file = await diskFileHandle.getFile();
                      triggerSave(file, currentFile.name);
                    }
                  } else if (swPipePort) {
                    swPipePort.postMessage("EOF");
                  } else {
                    let finalBlob;
                    if (useIndexedDB) {
                      finalBlob = await getAllChunksIDB(currentFile.mime);
                      await clearIDB();
                    } else {
                      finalBlob = new Blob(receivedChunks, { type: currentFile.mime });
                    }
                    triggerSave(finalBlob, currentFile.name);
                  }
                  resolve();
                } catch (err) {
                  hasError = true;
                  // Handled in chunkQueue onError callback
                }
              }
              return;
            }

            const value = new Uint8Array(e.data);
            let newBuffer = new Uint8Array(encBuffer.length + value.length);
            newBuffer.set(encBuffer, 0);
            newBuffer.set(value, encBuffer.length);
            encBuffer = newBuffer;

            while (encBuffer.length >= 4) {
              const dv = new DataView(encBuffer.buffer, encBuffer.byteOffset, encBuffer.byteLength);
              const frameLen = dv.getUint32(0, false);
              if (encBuffer.length >= 4 + frameLen) {
                const frame = encBuffer.slice(4, 4 + frameLen);
                encBuffer = encBuffer.slice(4 + frameLen);

                const nonce = new Uint8Array(frame.subarray(0, 12));
                const ciphertext = new Uint8Array(frame.subarray(12));
                const decrypted = await crypto.subtle.decrypt(
                  { name: "AES-GCM", iv: nonce },
                  decryptionKey,
                  ciphertext
                );
                const decValue = new Uint8Array(decrypted);
                chunkQueue.enqueue(decValue);
              } else {
                break;
              }
            }
          }).catch((err) => {
            showError(`Transfer failed: ${err.message}`);
            dc.close();
            reject(err);
          });
        } else {
          try {
            if (typeof e.data === 'string') {
              if (e.data === "EOF") {
                chunkQueue.enqueueEOF();
                chunkQueue.drain().then(async () => {
                  if (diskWritableStream) {
                    await diskWritableStream.close();
                    if (useOPFS) {
                      const file = await diskFileHandle.getFile();
                      triggerSave(file, currentFile.name);
                    }
                  } else if (swPipePort) {
                    swPipePort.postMessage("EOF");
                  } else {
                    let finalBlob;
                    if (useIndexedDB) {
                      finalBlob = await getAllChunksIDB(currentFile.mime);
                      await clearIDB();
                    } else {
                      finalBlob = new Blob(receivedChunks, { type: currentFile.mime });
                    }
                    triggerSave(finalBlob, currentFile.name);
                  }
                  resolve();
                }).catch((err) => {
                  // Handled in chunkQueue onError callback
                });
              }
              return;
            }

            const chunk = new Uint8Array(e.data);
            chunkQueue.enqueue(chunk);
          } catch (err) {
            showError(`Transfer failed: ${err.message}`);
            dc.close();
            reject(err);
          }
        }

        const chunk = new Uint8Array(e.data);
        msgChain = msgChain.then(async () => {
          if (isTerminated) return;

          if (decryptionKey) {
            let newBuffer = new Uint8Array(encBuffer.length + chunk.length);
            newBuffer.set(encBuffer, 0);
            newBuffer.set(chunk, encBuffer.length);
            encBuffer = newBuffer;

            while (encBuffer.length >= 4) {
              const dv = new DataView(encBuffer.buffer, encBuffer.byteOffset, encBuffer.byteLength);
              const frameLen = dv.getUint32(0, false);
              if (encBuffer.length >= 4 + frameLen) {
                const frame = encBuffer.slice(4, 4 + frameLen);
                encBuffer = encBuffer.slice(4 + frameLen);

                if (frameLen < 12) {
                  throw new Error("Invalid frame length: header smaller than nonce size");
                }

                const nonce = new Uint8Array(frame.subarray(0, 12));
                const ciphertext = new Uint8Array(frame.subarray(12));

                let decrypted;
                try {
                  decrypted = await crypto.subtle.decrypt(
                    { name: "AES-GCM", iv: nonce },
                    decryptionKey,
                    ciphertext
                  );
                } catch (decryptErr) {
                  throw new Error("Decryption failed: corrupted frame or invalid key");
                }

                const decValue = new Uint8Array(decrypted);
                chunkQueue.enqueue(decValue);
              } else {
                break;
              }
            }
          } else {
            chunkQueue.enqueue(chunk);
          }
        }).catch((err) => {
          if (isTerminated) return;
          isTerminated = true;
          showError(`Transfer failed: ${err.message}`);
          try { dc.close(); } catch (closeErr) {}
          reject(err);
        });
      };

      dc.onerror = (e) => reject(new Error('data channel error: ' + e));
      dc.onclose = () => {
        if (receivedBytes < totalBytes) reject(new Error('channel closed early'));
      };
    });

    let modeDesc = diskWritableStream ? (useOPFS ? 'WebRTC P2P (OPFS)' : 'WebRTC P2P (Direct Disk)') : (swPipePort ? 'WebRTC P2P (SW Pipe)' : (useIndexedDB ? 'WebRTC P2P (IndexedDB)' : 'WebRTC P2P (RAM Blob)'));
    showDone(currentFile.name, currentFile.size, modeDesc);
    pc.close();
  }
}

// ── WebRTC Buffer Backpressure Helper ─────────────────────────────────────────
/**
 * Helper function to wait for WebRTC DataChannel bufferedAmount to drop <= targetThreshold.
 * Attaches the 'bufferedamountlow' listener and immediately re-evaluates bufferedAmount before awaiting,
 * supplemented by a polling fallback to prevent race conditions during buffer drains.
 */
function waitForBufferedAmountLow(dc, targetThreshold = 0, pollMs = 25) {
  return new Promise((resolve, reject) => {
    if (!dc || dc.readyState !== 'open') {
      return reject(new Error("Data channel is closed or closing"));
    }

    dc.bufferedAmountLowThreshold = targetThreshold;

    if (dc.bufferedAmount <= targetThreshold) {
      return resolve();
    }

    let intervalId = null;

    const cleanup = () => {
      if (dc && typeof dc.removeEventListener === 'function') {
        dc.removeEventListener('bufferedamountlow', onBufferedAmountLow);
      }
      if (intervalId !== null) {
        clearInterval(intervalId);
        intervalId = null;
      }
    };

    const onBufferedAmountLow = () => {
      cleanup();
      resolve();
    };

    // Attach bufferedamountlow listener
    dc.addEventListener('bufferedamountlow', onBufferedAmountLow);

    // Immediately re-evaluate bufferedAmount after attaching listener
    if (dc.bufferedAmount <= targetThreshold) {
      cleanup();
      return resolve();
    }

    // Polling fallback to check for buffer drain or closed channel
    intervalId = setInterval(() => {
      if (dc.readyState !== 'open') {
        cleanup();
        reject(new Error("Data channel is closed or closing"));
        return;
      }
      if (dc.bufferedAmount <= targetThreshold) {
        cleanup();
        resolve();
      }
    }, pollMs);
  });
}

function waitForDataChannelBuffer(dc, highWatermark = 1024 * 1024, lowWatermark = 512 * 1024, pollMs = 250) {
  return waitForBufferedAmountLow(dc, lowWatermark, pollMs);
}

// ── Phone-to-Laptop Upload Handler ───────────────────────────────────────────
async function uploadFileP2P(file, dc = webrtcDataChannel) {
  if (!dc || dc.readyState !== 'open') {
    throw new Error("Data channel is not open");
  }

  dc.send("UPLOAD_META:" + file.name + ":" + file.size);
  const dlLabelEl = document.getElementById('dl-label');
  if (dlLabelEl) dlLabelEl.textContent = "Uploading to Laptop…";
  setState('downloading');
  startTime = Date.now();
  receivedBytes = 0;
  updateProgress(0);

  const chunkSize = 65536;
  let offset = 0;
  const total = file.size;

  while (offset < total) {
    const chunkBlob = file.slice(offset, offset + chunkSize);
    const chunkBuffer = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = reject;
      reader.readAsArrayBuffer(chunkBlob);
    });

    await waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);
    dc.send(chunkBuffer);
    offset += chunkBuffer.byteLength;

    updateProgress(offset / total);
    updateDLStats(offset, total);
    updateSpeed(offset);
  }

  await waitForDataChannelBuffer(dc, 0, 0);
  dc.send("UPLOAD_EOF");
  showDone(file.name, file.size, "WebRTC P2P Upload");
}

async function handleUploadFile(e) {
  const file = e.target.files[0];
  if (!file) return;

  if (transferMode === 'webrtc' && webrtcDataChannel && webrtcDataChannel.readyState === 'open') {
    webrtcDataChannel.send("UPLOAD_META:" + file.name + ":" + file.size);
    document.getElementById('dl-label').textContent = "Uploading to Laptop…";
    setState('downloading');
    startTime = Date.now();
    receivedBytes = 0;
    updateProgress(0);

    const chunkSize = 65536;
    let offset = 0;
    const total = file.size;

    while (offset < total) {
      if (webrtcDataChannel.readyState !== 'open') {
        throw new Error("Data channel is no longer open");
      }
      const chunkBlob = file.slice(offset, offset + chunkSize);
      const chunkBuffer = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.onerror = reject;
        reader.readAsArrayBuffer(chunkBlob);
      });

      if (webrtcDataChannel.bufferedAmount > 1024 * 1024) {
        await waitForBufferedAmountLow(webrtcDataChannel, 512 * 1024);
      }
      webrtcDataChannel.send(chunkBuffer);
      offset += chunkBuffer.byteLength;

      updateProgress(offset / total);
      updateDLStats(offset, total);
      updateSpeed(offset);
    }

    if (webrtcDataChannel.bufferedAmount > 0) {
      await waitForBufferedAmountLow(webrtcDataChannel, 0);
    }
    webrtcDataChannel.send("UPLOAD_EOF");
    showDone(file.name, file.size, "WebRTC P2P Upload");
  } else {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append('file', file);

    document.getElementById('dl-label').textContent = "Uploading to Laptop…";
    setState('downloading');
    startTime = Date.now();
    updateProgress(0);

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        updateProgress(event.loaded / event.total);
        updateDLStats(event.loaded, event.total);
        updateSpeed(event.loaded);
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        showDone(file.name, file.size, "HTTP Upload");
      } else {
        showError("Upload failed with status: " + xhr.status);
      }
    };

    xhr.onerror = () => {
      showError("Upload failed due to network error.");
    };

    xhr.open('POST', apiPath('/api/upload'), true);
    xhr.send(formData);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function renderFileCard(meta) {
  document.getElementById('file-name').textContent = meta.name;
  document.getElementById('file-size').textContent = formatBytes(meta.size);
  document.getElementById('file-mime').textContent = mimeLabel(meta.mime);
  document.getElementById('file-icon-wrap').innerHTML = mimeIcon(meta.mime);
}

function triggerSave(blob, name) {
  const url = URL.createObjectURL(blob);
  const a   = Object.assign(document.createElement('a'), { href: url, download: name, target: '_blank', rel: 'noopener' });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    if (a.parentNode) {
      a.parentNode.removeChild(a);
    }
    URL.revokeObjectURL(url);
  }, 5000);
}

function appendTerminalText(text) {
  if (virtualViewer) virtualViewer.append(text);
  receivedBytes += text.length;
}

function showDone(name, size, mode) {
  try { if (typeof localStorage !== 'undefined') localStorage.removeItem('beam_resume'); } catch (_) {}
  document.getElementById('done-sub').textContent = `${name} · ${formatBytes(size)}`;
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const speed   = formatBytes(size / (elapsed || 1)) + '/s';
  document.getElementById('done-meta').innerHTML =
    `<span>${mode}</span><span>${elapsed}s · avg ${speed}</span>`;

  const doneTitle = document.getElementById('done-title');
  const doneShare = document.getElementById('done-share-container');

  if (mode.includes("Upload")) {
    if (doneTitle) doneTitle.textContent = "File Shared Successfully!";
    
    // Create sharing link pointing to the laptop's file server
    let shareLink = window.location.origin;
    if (transferMode === 'webrtc') {
      shareLink += "?mode=webrtc";
    }
    currentShareURL = shareLink;

    // Load QR SVG data URL locally without external network requests
    const qrImg = document.getElementById('done-qr-img');
    if (qrImg) {
      qrImg.src = generateQRCodeDataURL(shareLink);
    }
    
    if (doneShare) doneShare.classList.remove('hidden');
  } else {
    if (doneTitle) doneTitle.textContent = "Transfer complete";
    if (doneShare) doneShare.classList.add('hidden');
  }

  setState('done');
}

function showError(msg) {
  if (useIndexedDB) clearIDB().catch(console.error);
  if (useOPFS) {
    navigator.storage?.getDirectory().then(root => root.removeEntry('beam_temp').catch(()=>{})).catch(()=>{});
  }
  document.getElementById('error-msg').textContent = msg;
  setState('error');
}

// ── Progress ──────────────────────────────────────────────────────────────────
function updateProgress(fraction) {
  const pct    = Math.round(fraction * 100);
  const circle = document.getElementById('progress-circle');
  const label  = document.getElementById('progress-pct');
  const wrap   = document.getElementById('progress-wrap');
  if (circle) circle.style.strokeDashoffset = CIRCUMFERENCE * (1 - fraction);
  if (label)  label.textContent = `${pct}%`;
  if (wrap)   wrap.setAttribute('aria-valuenow', pct);
}

function updateDLStats(received, total) {
  const el = document.getElementById('dl-stats');
  if (el) el.textContent = `${formatBytes(received)} / ${formatBytes(total)}`;
}

let lastSpeedSample = { t: 0, b: 0 };
function updateSpeed(received) {
  const now   = Date.now();
  const dt    = (now - lastSpeedSample.t) / 1000;
  if (dt < 0.5) return;
  const speed = (received - lastSpeedSample.b) / dt;
  lastSpeedSample = { t: now, b: received };
  const el = document.getElementById('dl-speed');
  if (el) el.textContent = formatBytes(speed) + '/s';
}

// ── Spotlight card effect ─────────────────────────────────────────────────────
function initSpotlight() {
  const card = document.getElementById('file-card');
  if (!card) return;
  card.addEventListener('mousemove', (e) => {
    const r = card.getBoundingClientRect();
    card.style.setProperty('--mouse-x', `${((e.clientX - r.left) / r.width * 100)}%`);
    card.style.setProperty('--mouse-y', `${((e.clientY - r.top)  / r.height * 100)}%`);
  });
}

// ── Local QR Code Generator ───────────────────────────────────────────────────
const GF256_EXP = new Uint8Array(512);
const GF256_LOG = new Uint8Array(256);
(function initGF256() {
  let x = 1;
  for (let i = 0; i < 255; i++) {
    GF256_EXP[i] = x;
    GF256_EXP[i + 255] = x;
    GF256_LOG[x] = i;
    x <<= 1;
    if (x & 256) x ^= 285;
  }
})();

function gfMul(x, y) {
  if (x === 0 || y === 0) return 0;
  return GF256_EXP[GF256_LOG[x] + GF256_LOG[y]];
}

function rsPolyMul(p1, p2) {
  const result = new Uint8Array(p1.length + p2.length - 1);
  for (let i = 0; i < p1.length; i++) {
    for (let j = 0; j < p2.length; j++) {
      result[i + j] ^= gfMul(p1[i], p2[j]);
    }
  }
  return result;
}

function rsGenPoly(numEc) {
  let g = new Uint8Array([1]);
  for (let i = 0; i < numEc; i++) {
    g = rsPolyMul(g, new Uint8Array([1, GF256_EXP[i]]));
  }
  return g;
}

function rsComputeSyndromes(data, numEc) {
  const gen = rsGenPoly(numEc);
  const msg = new Uint8Array(data.length + numEc);
  msg.set(data);
  for (let i = 0; i < data.length; i++) {
    const coef = msg[i];
    if (coef !== 0) {
      for (let j = 0; j < gen.length; j++) {
        msg[i + j] ^= gfMul(gen[j], coef);
      }
    }
  }
  return msg.slice(data.length);
}

const RS_BLOCK_TABLE_L = [
  [19, 7, 1, 19, 0, 0], [34, 10, 1, 34, 0, 0], [55, 15, 1, 55, 0, 0], [80, 20, 1, 80, 0, 0],
  [108, 26, 1, 108, 0, 0], [136, 18, 2, 68, 0, 0], [156, 20, 2, 78, 0, 0], [194, 24, 2, 97, 0, 0],
  [232, 30, 2, 116, 0, 0], [274, 18, 2, 68, 2, 69], [324, 20, 4, 81, 0, 0], [370, 24, 2, 92, 2, 93],
  [428, 26, 4, 107, 0, 0], [461, 30, 3, 115, 1, 116], [523, 22, 5, 87, 1, 88], [586, 24, 5, 98, 1, 99],
  [647, 28, 1, 107, 5, 108], [721, 30, 5, 120, 1, 121], [795, 28, 3, 113, 4, 114], [868, 28, 3, 107, 5, 108],
  [926, 28, 4, 115, 4, 116], [1002, 28, 2, 125, 6, 126], [1091, 30, 4, 121, 5, 122], [1171, 30, 6, 117, 4, 118],
  [1277, 26, 8, 106, 4, 107], [1367, 28, 10, 114, 2, 115], [1465, 28, 8, 122, 4, 123], [1528, 30, 3, 117, 10, 118],
  [1628, 30, 7, 116, 7, 117], [1732, 30, 5, 115, 10, 116], [1840, 30, 13, 115, 3, 116], [1952, 30, 17, 115, 0, 0],
  [2068, 30, 17, 115, 1, 116], [2188, 30, 19, 115, 1, 116], [2303, 30, 6, 115, 14, 116], [2431, 30, 6, 115, 15, 116],
  [2563, 30, 17, 115, 5, 116], [2699, 30, 4, 115, 19, 116], [2809, 30, 20, 115, 4, 116], [2953, 30, 19, 115, 6, 116]
];

const ALIGNMENT_POS = [
  [], [6, 18], [6, 22], [6, 26], [6, 30], [6, 34],
  [6, 22, 38], [6, 24, 42], [6, 26, 46], [6, 28, 50], [6, 30, 54],
  [6, 32, 58], [6, 34, 62], [6, 26, 46, 66], [6, 26, 48, 70], [6, 26, 50, 74],
  [6, 30, 54, 78], [6, 30, 56, 82], [6, 30, 58, 86], [6, 34, 62, 90],
  [6, 28, 50, 72, 94], [6, 26, 50, 74, 98], [6, 30, 54, 78, 102], [6, 28, 54, 80, 106],
  [6, 32, 58, 84, 110], [6, 30, 58, 86, 114], [6, 34, 62, 90, 118], [6, 26, 50, 74, 98, 122],
  [6, 30, 54, 78, 102, 126], [6, 26, 52, 78, 104, 130], [6, 30, 56, 82, 108, 134],
  [6, 34, 60, 86, 112, 138], [6, 30, 58, 86, 114, 142], [6, 34, 62, 90, 118, 146],
  [6, 30, 54, 78, 102, 126, 150], [6, 24, 50, 76, 102, 128, 154], [6, 28, 54, 80, 106, 132, 158],
  [6, 32, 58, 84, 110, 136, 162], [6, 26, 54, 82, 110, 138, 166], [6, 30, 58, 86, 114, 142, 170]
];

function getFormatInfo(ecLevelBit, maskPattern) {
  const data = (ecLevelBit << 3) | maskPattern;
  let rem = data << 10;
  for (let i = 4; i >= 0; i--) {
    if (rem & (1 << (i + 10))) {
      rem ^= (0x537 << i);
    }
  }
  return ((data << 10) | rem) ^ 0x5370;
}

function getVersionInfo(version) {
  let rem = version << 12;
  for (let i = 5; i >= 0; i--) {
    if (rem & (1 << (i + 12))) {
      rem ^= (0x1F25 << i);
    }
  }
  return (version << 12) | rem;
}

function generateQRCodeSVG(text) {
  let bytes;
  if (typeof TextEncoder !== 'undefined') {
    bytes = new TextEncoder().encode(text);
  } else {
    bytes = new Uint8Array(text.length);
    for (let i = 0; i < text.length; i++) {
      bytes[i] = text.charCodeAt(i) & 0xff;
    }
  }

  let version = 1;
  let spec = null;
  for (let v = 1; v <= 40; v++) {
    spec = RS_BLOCK_TABLE_L[v - 1];
    const totalDataCap = spec[0];
    const headerBits = 4 + (v >= 10 ? 16 : 8);
    const requiredBits = headerBits + bytes.length * 8;
    if (requiredBits <= totalDataCap * 8) {
      version = v;
      break;
    }
  }

  const specCap = spec[0];
  const ecPerBlock = spec[1];
  const g1Blocks = spec[2];
  const g1Data = spec[3];
  const g2Blocks = spec[4];
  const g2Data = spec[5];
  const totalBlocks = g1Blocks + g2Blocks;

  const bits = [];
  function pushBits(val, count) {
    for (let i = count - 1; i >= 0; i--) {
      bits.push((val >> i) & 1);
    }
  }

  pushBits(4, 4);
  pushBits(bytes.length, version >= 10 ? 16 : 8);
  for (let b of bytes) {
    pushBits(b, 8);
  }
  const totalBitsCap = specCap * 8;
  const termBits = Math.min(4, totalBitsCap - bits.length);
  pushBits(0, termBits);
  while (bits.length % 8 !== 0) {
    bits.push(0);
  }
  const padBytes = [0xEC, 0x11];
  let padIdx = 0;
  while (bits.length < totalBitsCap) {
    pushBits(padBytes[padIdx % 2], 8);
    padIdx++;
  }

  const dataCodewords = new Uint8Array(specCap);
  for (let i = 0; i < specCap; i++) {
    let byteVal = 0;
    for (let b = 0; b < 8; b++) {
      byteVal = (byteVal << 1) | bits[i * 8 + b];
    }
    dataCodewords[i] = byteVal;
  }

  const blocks = [];
  let cwOffset = 0;
  for (let b = 0; b < g1Blocks; b++) {
    const blockData = dataCodewords.slice(cwOffset, cwOffset + g1Data);
    cwOffset += g1Data;
    const ec = rsComputeSyndromes(blockData, ecPerBlock);
    blocks.push({ data: blockData, ec });
  }
  for (let b = 0; b < g2Blocks; b++) {
    const blockData = dataCodewords.slice(cwOffset, cwOffset + g2Data);
    cwOffset += g2Data;
    const ec = rsComputeSyndromes(blockData, ecPerBlock);
    blocks.push({ data: blockData, ec });
  }

  const finalCodewords = [];
  const maxDataLen = Math.max(g1Data, g2Data);
  for (let i = 0; i < maxDataLen; i++) {
    for (let b = 0; b < totalBlocks; b++) {
      if (i < blocks[b].data.length) {
        finalCodewords.push(blocks[b].data[i]);
      }
    }
  }
  for (let i = 0; i < ecPerBlock; i++) {
    for (let b = 0; b < totalBlocks; b++) {
      finalCodewords.push(blocks[b].ec[i]);
    }
  }

  const size = version * 4 + 17;
  const modules = Array.from({ length: size }, () => new Uint8Array(size));
  const isFunction = Array.from({ length: size }, () => new Uint8Array(size));

  function placeFinder(r, c) {
    for (let dr = -1; dr <= 7; dr++) {
      for (let dc = -1; dc <= 7; dc++) {
        const nr = r + dr;
        const nc = c + dc;
        if (nr >= 0 && nr < size && nc >= 0 && nc < size) {
          isFunction[nr][nc] = 1;
          if (dr >= 0 && dr <= 6 && dc >= 0 && dc <= 6) {
            if (dr === 0 || dr === 6 || dc === 0 || dc === 6 || (dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4)) {
              modules[nr][nc] = 1;
            } else {
              modules[nr][nc] = 0;
            }
          } else {
            modules[nr][nc] = 0;
          }
        }
      }
    }
  }
  placeFinder(0, 0);
  placeFinder(0, size - 7);
  placeFinder(size - 7, 0);

  const alignCoords = ALIGNMENT_POS[version - 1];
  for (let r of alignCoords) {
    for (let c of alignCoords) {
      if (isFunction[r][c]) continue;
      for (let dr = -2; dr <= 2; dr++) {
        for (let dc = -2; dc <= 2; dc++) {
          const nr = r + dr;
          const nc = c + dc;
          isFunction[nr][nc] = 1;
          if (Math.abs(dr) === 2 || Math.abs(dc) === 2 || (dr === 0 && dc === 0)) {
            modules[nr][nc] = 1;
          } else {
            modules[nr][nc] = 0;
          }
        }
      }
    }
  }

  for (let i = 8; i < size - 8; i++) {
    if (!isFunction[6][i]) {
      isFunction[6][i] = 1;
      modules[6][i] = (i % 2 === 0) ? 1 : 0;
    }
    if (!isFunction[i][6]) {
      isFunction[i][6] = 1;
      modules[i][6] = (i % 2 === 0) ? 1 : 0;
    }
  }

  isFunction[size - 8][8] = 1;
  modules[size - 8][8] = 1;

  for (let i = 0; i < 9; i++) {
    if (i !== 6) {
      isFunction[8][i] = 1;
      isFunction[i][8] = 1;
    }
  }
  for (let i = 0; i < 8; i++) {
    isFunction[8][size - 1 - i] = 1;
    isFunction[size - 1 - i][8] = 1;
  }

  if (version >= 7) {
    for (let r = 0; r < 6; r++) {
      for (let c = 0; c < 3; c++) {
        isFunction[r][size - 11 + c] = 1;
        isFunction[size - 11 + c][r] = 1;
      }
    }
  }

  const flatBits = [];
  for (let cw of finalCodewords) {
    for (let i = 7; i >= 0; i--) {
      flatBits.push((cw >> i) & 1);
    }
  }

  let bitIdx = 0;
  let up = true;
  for (let right = size - 1; right > 0; right -= 2) {
    if (right === 6) right--;
    const rows = [];
    if (up) {
      for (let r = size - 1; r >= 0; r--) rows.push(r);
    } else {
      for (let r = 0; r < size; r++) rows.push(r);
    }
    for (let r of rows) {
      for (let c of [right, right - 1]) {
        if (!isFunction[r][c]) {
          if (bitIdx < flatBits.length) {
            modules[r][c] = flatBits[bitIdx++];
          }
        }
      }
    }
    up = !up;
  }

  const mask = 0;
  for (let r = 0; r < size; r++) {
    for (let c = 0; c < size; c++) {
      if (!isFunction[r][c]) {
        if ((r + c) % 2 === 0) {
          modules[r][c] ^= 1;
        }
      }
    }
  }

  const formatInfo = getFormatInfo(1, mask);
  const formatBits = [];
  for (let i = 14; i >= 0; i--) {
    formatBits.push((formatInfo >> i) & 1);
  }

  const formatCoordsTopLeft = [
    [8, 0], [8, 1], [8, 2], [8, 3], [8, 4], [8, 5], [8, 7], [8, 8],
    [7, 8], [5, 8], [4, 8], [3, 8], [2, 8], [1, 8], [0, 8]
  ];
  const formatCoordsSplit = [
    [size - 1, 8], [size - 2, 8], [size - 3, 8], [size - 4, 8], [size - 5, 8], [size - 6, 8], [size - 7, 8],
    [8, size - 8], [8, size - 7], [8, size - 6], [8, size - 5], [8, size - 4], [8, size - 3], [8, size - 2], [8, size - 1]
  ];

  for (let i = 0; i < 15; i++) {
    const [r1, c1] = formatCoordsTopLeft[i];
    modules[r1][c1] = formatBits[i];
    const [r2, c2] = formatCoordsSplit[i];
    modules[r2][c2] = formatBits[i];
  }

  if (version >= 7) {
    const verInfo = getVersionInfo(version);
    for (let i = 0; i < 18; i++) {
      const bit = (verInfo >> i) & 1;
      const r1 = Math.floor(i / 3);
      const c1 = size - 11 + (i % 3);
      modules[r1][c1] = bit;
      modules[c1][r1] = bit;
    }
  }

  const margin = 4;
  const totalSize = size + margin * 2;
  let pathD = "";
  for (let r = 0; r < size; r++) {
    for (let c = 0; c < size; c++) {
      if (modules[r][c]) {
        const x = c + margin;
        const y = r + margin;
        pathD += `M${x},${y}h1v1h-1z`;
      }
    }
  }

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${totalSize} ${totalSize}" width="100%" height="100%"><rect width="${totalSize}" height="${totalSize}" fill="#ffffff"/><path d="${pathD}" fill="#000000"/></svg>`;
  return "data:image/svg+xml;charset=utf-8," + encodeURIComponent(svg);
}

// ── Format helpers ────────────────────────────────────────────────────────────
function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function mimeLabel(mime) {
  const labels = {
    'application/pdf':'PDF','application/zip':'ZIP','application/gzip':'GZ',
    'application/x-tar':'TAR','video/mp4':'MP4','video/x-matroska':'MKV',
    'audio/mpeg':'MP3','image/png':'PNG','image/jpeg':'JPEG','image/gif':'GIF',
    'image/webp':'WEBP','text/plain':'TXT','text/markdown':'MD',
    'text/html':'HTML','application/json':'JSON','text/csv':'CSV',
  };
  return labels[mime] || 'FILE';
}

function mimeIcon(mime) {
  const a = `fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"`;
  if (mime?.startsWith('image/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>`;
  if (mime?.startsWith('video/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>`;
  if (mime?.startsWith('audio/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>`;
  if (mime === 'application/pdf')
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`;
  if (mime?.includes('zip')||mime?.includes('tar')||mime?.includes('gzip'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>`;
  return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`;
}


// ── Web Sender States & Functions ──
let senderFile = null;
let senderPeerConnection = null;
let senderDataChannel = null;
let senderSessionID = null;
let isSenderPolling = false;
let senderAborted = false;
let senderEncryptionKey = null;

function handleSenderFileSelect(file) {
  if (!file) return;
  senderFile = file;
  document.getElementById('sender-file-name').textContent = file.name;
  document.getElementById('sender-file-size').textContent = formatBytes(file.size);
  document.getElementById('sender-file-icon-wrap').innerHTML = mimeIcon(file.type);
  setState('send-ready');
}

async function startSenderSharing() {
  senderAborted = false;
  setState('loading');
  setLoadingSub('Registering session on relay server…');

  const backend = getBackendURL();

  try {
    const regRes = await fetch(`${backend}/relay/register`);
    if (!regRes.ok) throw new Error(`Register failed: HTTP ${regRes.status}`);
    const regData = await regRes.json();
    senderSessionID = regData.session;

    setLoadingSub('Creating WebRTC peer connection…');
    senderPeerConnection = new RTCPeerConnection({ iceServers: getIceServers({}) });
    
    const localCandidates = [];
    senderPeerConnection.onicecandidate = (e) => {
      if (e.candidate) {
        localCandidates.push(e.candidate.toJSON());
      }
    };

    senderDataChannel = senderPeerConnection.createDataChannel("beamshare", { ordered: true });
    setupSenderDataChannel();

    const offer = await senderPeerConnection.createOffer();
    await senderPeerConnection.setLocalDescription(offer);

    await new Promise(resolve => setTimeout(resolve, 2000));

    setLoadingSub('Publishing SDP offer to relay…');
    const stateRes = await fetch(`${backend}/relay/state?session=${senderSessionID}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        offer: senderPeerConnection.localDescription.sdp,
        candidates: localCandidates,
        meta: {
          name: senderFile.name,
          size: senderFile.size,
          mime: senderFile.type || 'application/octet-stream'
        }
      })
    });
    if (!stateRes.ok) throw new Error(`Publish state failed: HTTP ${stateRes.status}`);

    const shareURL = new URL(window.location.origin);
    shareURL.searchParams.set('s', senderSessionID);
    shareURL.searchParams.set('backend', backend);
    shareURL.searchParams.set('mode', 'webrtc');

    // Generate AES-GCM Key
    senderEncryptionKey = await crypto.subtle.generateKey(
      { name: "AES-GCM", length: 256 },
      true,
      ["encrypt", "decrypt"]
    );
    const rawKey = await crypto.subtle.exportKey("raw", senderEncryptionKey);
    const keyB64 = btoa(String.fromCharCode.apply(null, new Uint8Array(rawKey)))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+/g, '');
    shareURL.hash = `k=${keyB64}`;

    document.getElementById('send-url-input').value = shareURL.href;
    const sendQrImg = document.getElementById('send-qr-img');
    if (sendQrImg) {
      renderQRCode(shareURL.href, sendQrImg);
    }
    const sendQrCanvas = document.getElementById('send-qr-canvas');
    if (sendQrCanvas) {
      renderQRCode(shareURL.href, sendQrCanvas);
    }
    
    document.getElementById('send-link-section').classList.remove('hidden');
    document.getElementById('send-progress-section').classList.add('hidden');
    document.getElementById('send-status-label').textContent = "Waiting for receiver…";

    setMode('sender', 'P2P Sender Mode');
    setState('send-sharing');

    startSenderPolling(backend);

  } catch (err) {
    showError(`Sender setup failed: ${err.message}`);
  }
}

let senderPaused = false;

function setupSenderDataChannel() {
  senderDataChannel.onopen = () => {
    console.log("WebRTC data channel is open!");
    document.getElementById('send-link-section').classList.add('hidden');
    document.getElementById('send-progress-section').classList.remove('hidden');
    document.getElementById('send-status-label').textContent = "Connecting to peer…";
  };

  let reverseStream = null;
  let reverseFileHandle = null;
  let reverseReceived = 0;
  let reverseTotal = 0;
  let reverseName = "";
  let reverseChunks = [];

  senderDataChannel.onmessage = async (e) => {
    if (typeof e.data === 'string') {
      if (e.data.startsWith('OFFSET:')) {
        const offset = parseInt(e.data.split(':')[1], 10);
        document.getElementById('send-status-label').textContent = "Uploading file…";
        try {
          await streamFileToDataChannel(offset);
        } catch (err) {
          console.error("WebRTC streaming failed:", err);
        }
      } else if (e.data === 'PAUSE') {
        senderPaused = true;
      } else if (e.data === 'RESUME') {
        senderPaused = false;
      } else if (e.data.startsWith('UPLOAD_META:')) {
        const parts = e.data.split(':');
        reverseName = parts[1];
        reverseTotal = parseInt(parts[2], 10);
        reverseReceived = 0;
        reverseChunks = [];
        
        document.getElementById('send-link-section').classList.add('hidden');
        document.getElementById('send-progress-section').classList.remove('hidden');
        document.getElementById('send-status-label').textContent = "Receiving P2P file…";
        
        if (typeof window.showSaveFilePicker === 'function') {
          try {
            reverseFileHandle = await window.showSaveFilePicker({ suggestedName: reverseName });
            reverseStream = await reverseFileHandle.createWritable();
          } catch (err) {
            console.warn("Save file picker cancelled or failed", err);
          }
        }
      } else if (e.data === 'UPLOAD_EOF') {
        if (reverseStream) {
          await reverseStream.close();
        } else {
          const blob = new Blob(reverseChunks);
          triggerSave(blob, reverseName);
        }
        document.getElementById('send-status-label').textContent = "Reverse Transfer Complete!";
      }
    } else {
      // Binary chunk
      const chunk = new Uint8Array(e.data);
      if (reverseStream) {
        await reverseStream.write(chunk);
      } else {
        reverseChunks.push(chunk);
      }
      reverseReceived += chunk.byteLength;
      
      const progressCircle = document.getElementById('send-progress-circle');
      const progressPct = document.getElementById('send-progress-pct');
      const statsEl = document.getElementById('send-stats');
      
      if (reverseTotal > 0) {
        const progress = reverseReceived / reverseTotal;
        if (progressPct) progressPct.textContent = `${Math.round(progress * 100)}%`;
        if (progressCircle) {
          progressCircle.style.strokeDashoffset = 263.9 - (263.9 * progress);
        }
        if (statsEl) statsEl.textContent = `${formatBytes(reverseReceived)} / ${formatBytes(reverseTotal)}`;
      }
    }
  };

  senderDataChannel.onclose = () => {
    console.log("Data channel closed");
  };
}

async function sendWebRTCFile(initialOffset = 0, dc = senderDataChannel) {
  senderPaused = false;
  const chunkSize = 65536;
  let offset = initialOffset;
  const total = senderFile.size;

  const progressCircle = document.getElementById('send-progress-circle');
  const progressPct = document.getElementById('send-progress-pct');
  const statsEl = document.getElementById('send-stats');

  while (offset < total && !senderAborted) {
    if (!dc || dc.readyState !== 'open') {
      throw new Error("Data channel is no longer open");
    }

    const chunkBlob = senderFile.slice(offset, offset + chunkSize);
    const chunkBuffer = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = reject;
      reader.readAsArrayBuffer(chunkBlob);
    });

    while (dc && (dc.bufferedAmount > 1024 * 1024 || senderPaused)) {
      if (dc.readyState !== 'open') throw new Error("Data channel is no longer open");
      if (dc.bufferedAmount > 1024 * 1024) {
        await waitForBufferedAmountLow(dc, 512 * 1024);
      } else if (senderPaused) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }

    let payload;
    if (senderEncryptionKey) {
      const nonce = crypto.getRandomValues(new Uint8Array(12));
      const ciphertext = await crypto.subtle.encrypt(
        { name: "AES-GCM", iv: nonce },
        senderEncryptionKey,
        chunkBuffer
      );
      const frameLen = 12 + ciphertext.byteLength;
      payload = new Uint8Array(4 + frameLen);
      const dv = new DataView(payload.buffer);
      dv.setUint32(0, frameLen, false); // Big endian
      payload.set(nonce, 4);
      payload.set(new Uint8Array(ciphertext), 4 + 12);
    } else {
      payload = chunkBuffer;
    }

    dc.send(payload);
    offset += chunkBuffer.byteLength;

    const progress = offset / total;
    if (progressPct) progressPct.textContent = `${Math.round(progress * 100)}%`;
    if (progressCircle) {
      const strokeDashoffset = 263.9 - (263.9 * progress);
      progressCircle.style.strokeDashoffset = strokeDashoffset;
    }
    if (statsEl) {
      statsEl.textContent = `${formatBytes(offset)} / ${formatBytes(total)}`;
    }
  }

  if (senderAborted) return;

  if (dc && dc.bufferedAmount > 0) {
    await waitForBufferedAmountLow(dc, 0);
  }
  dc.send("EOF");
  document.getElementById('send-status-label').textContent = "Transfer Complete!";
}

async function startSenderPolling(backend) {
  if (isSenderPolling) return;
  isSenderPolling = true;

  while (isSenderPolling && !senderAborted) {
    try {
      const pollRes = await fetch(`${backend}/relay/poll?session=${senderSessionID}`);
      if (!pollRes.ok) {
        if (pollRes.status === 404 || pollRes.status === 410) {
          break;
        }
        await new Promise(resolve => setTimeout(resolve, 2000));
        continue;
      }

      const data = await pollRes.json();
      if (data.action === 'answer') {
        await senderPeerConnection.setRemoteDescription(new RTCSessionDescription({
          type: 'answer',
          sdp: data.answer
        }));
      } else if (data.action === 'download') {
        document.getElementById('send-link-section').classList.add('hidden');
        document.getElementById('send-progress-section').classList.remove('hidden');
        document.getElementById('send-status-label').textContent = "Streaming via HTTP relay…";
        await streamFileToHTTP(backend);
        break;
      } else if (data.action === 'upload') {
        document.getElementById('send-link-section').classList.add('hidden');
        document.getElementById('send-progress-section').classList.remove('hidden');
        document.getElementById('send-status-label').textContent = "Receiving file from relay…";
        await receiveFileFromHTTP(backend, data.filename);
        break;
      }
    } catch (err) {
      console.warn("Polling error:", err);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  isSenderPolling = false;
}

async function streamFileToHTTP(backend) {
  const progressCircle = document.getElementById('send-progress-circle');
  const progressPct = document.getElementById('send-progress-pct');
  const statsEl = document.getElementById('send-stats');

  try {
    const xhr = new XMLHttpRequest();
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        const progress = e.loaded / e.total;
        if (progressPct) progressPct.textContent = `${Math.round(progress * 100)}%`;
        if (progressCircle) {
          const strokeDashoffset = 263.9 - (263.9 * progress);
          progressCircle.style.strokeDashoffset = strokeDashoffset;
        }
        if (statsEl) {
          statsEl.textContent = `${formatBytes(e.loaded)} / ${formatBytes(e.total)}`;
        }
      }
    };

    const uploadPromise = new Promise((resolve, reject) => {
      xhr.onload = () => {
        if (xhr.status === 200) {
          document.getElementById('send-status-label').textContent = "Transfer Complete!";
          resolve();
        } else {
          reject(new Error(`HTTP ${xhr.status}`));
        }
      };
      xhr.onerror = () => reject(new Error("Network error during HTTP fallback upload"));
    });

    xhr.open('POST', `${backend}/relay/data?session=${senderSessionID}`, true);
    xhr.send(senderFile);
    await uploadPromise;

  } catch (err) {
    showError(`HTTP upload failed: ${err.message}`);
  }
}

async function receiveFileFromHTTP(backend, filename) {
  const url = `${backend}/relay/pull?session=${senderSessionID}`;
  
  try {
    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    let writableStream = null;
    let fileHandle = null;
    
    if (useDiskStream) {
      try {
        fileHandle = await window.showSaveFilePicker({ suggestedName: filename });
        writableStream = await fileHandle.createWritable();
      } catch (e) {
        console.warn("Save file picker cancelled or failed", e);
      }
    }
    
    if (!writableStream) {
      // Fallback: direct download using an anchor tag which offloads streaming to the browser
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      document.getElementById('send-status-label').textContent = "Download started in browser!";
      return;
    }

    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    
    document.getElementById('send-status-label').textContent = "Downloading file…";
    const reader = res.body.getReader();
    let received = 0;
    
    const progressCircle = document.getElementById('send-progress-circle');
    const progressPct = document.getElementById('send-progress-pct');
    const statsEl = document.getElementById('send-stats');
    const total = Number(res.headers.get('Content-Length')) || 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      await writableStream.write(value);
      received += value.length;
      
      if (total > 0) {
        const progress = received / total;
        if (progressPct) progressPct.textContent = `${Math.round(progress * 100)}%`;
        if (progressCircle) {
          progressCircle.style.strokeDashoffset = 263.9 - (263.9 * progress);
        }
        if (statsEl) statsEl.textContent = `${formatBytes(received)} / ${formatBytes(total)}`;
      } else {
        if (statsEl) statsEl.textContent = formatBytes(received);
      }
    }
    await writableStream.close();
    document.getElementById('send-status-label').textContent = "Transfer Complete!";
  } catch (err) {
    showError(`HTTP download failed: ${err.message}`);
  }
}

function stopSenderSharing() {
  senderAborted = true;
  isSenderPolling = false;
  
  if (senderDataChannel) {
    try { senderDataChannel.close(); } catch(e){}
    senderDataChannel = null;
  }
  if (senderPeerConnection) {
    try { senderPeerConnection.close(); } catch(e){}
    senderPeerConnection = null;
  }
  
  resetState();
  switchTab('send');
}

function switchTab(tab) {
  const receiveBtn = document.getElementById('tab-receive');
  const sendBtn = document.getElementById('tab-send');
  if (tab === 'receive') {
    receiveBtn?.classList.add('active');
    sendBtn?.classList.remove('active');
    bootstrap();
  } else {
    sendBtn?.classList.add('active');
    receiveBtn?.classList.remove('active');
    setState('send-home');
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────
if (typeof window !== 'undefined' && !window.__BEAM_TEST_ENV__) {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
}

if (typeof window !== 'undefined') {
  window.addEventListener('pagehide', () => {
    if (useOPFS) {
      navigator.storage?.getDirectory().then(root => root.removeEntry('beam_temp').catch(()=>{})).catch(()=>{});
    }
    if (useIndexedDB && idb) {
      // Attempt best-effort synchronous-like clear
      try {
        const tx = idb.transaction(IDB_STORE, 'readwrite');
        tx.objectStore(IDB_STORE).clear();
      } catch (e) {
        // Ignore errors on unload
      }
    }
  });
}

// ── Client-side QR Code Generator ─────────────────────────────────────────────
const GF256_EXP = new Uint8Array(512);
const GF256_LOG = new Uint8Array(256);
(function initGF256() {
  let x = 1;
  for (let i = 0; i < 255; i++) {
    GF256_EXP[i] = x;
    GF256_LOG[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  for (let i = 255; i < 512; i++) {
    GF256_EXP[i] = GF256_EXP[i - 255];
  }
})();

function gfMul(x, y) {
  if (x === 0 || y === 0) return 0;
  return GF256_EXP[GF256_LOG[x] + GF256_LOG[y]];
}

function rsGeneratorPoly(degree) {
  let poly = [1];
  for (let i = 0; i < degree; i++) {
    const nextPoly = new Array(poly.length + 1).fill(0);
    const alpha = GF256_EXP[i];
    for (let j = 0; j < poly.length; j++) {
      nextPoly[j] ^= gfMul(poly[j], alpha);
      nextPoly[j + 1] ^= poly[j];
    }
    poly = nextPoly;
  }
  return poly;
}

function rsRemainder(data, eccCount) {
  const gen = rsGeneratorPoly(eccCount);
  const res = new Uint8Array(data.length + eccCount);
  res.set(data);
  for (let i = 0; i < data.length; i++) {
    const coef = res[i];
    if (coef !== 0) {
      for (let j = 0; j < gen.length; j++) {
        res[i + j] ^= gfMul(gen[j], coef);
      }
    }
  }
  return res.slice(data.length);
}

const QR_VERSIONS_L = [
  null,
  [1, 21, 19, [[1, 19]], 7],
  [2, 25, 34, [[1, 34]], 10],
  [3, 29, 55, [[1, 55]], 15],
  [4, 33, 80, [[1, 80]], 20],
  [5, 37, 108, [[1, 108]], 26],
  [6, 41, 136, [[2, 68]], 18],
  [7, 45, 156, [[2, 78]], 20],
  [8, 49, 194, [[2, 97]], 24],
  [9, 53, 232, [[2, 116]], 30],
  [10, 57, 274, [[2, 68], [2, 69]], 18],
  [11, 61, 324, [[4, 81]], 20],
  [12, 65, 370, [[2, 92], [2, 93]], 24],
  [13, 69, 428, [[4, 107]], 26],
  [14, 73, 461, [[3, 115], [1, 116]], 30],
  [15, 77, 523, [[5, 87], [1, 88]], 22],
  [16, 81, 586, [[5, 97], [1, 98]], 24],
  [17, 85, 644, [[1, 107], [5, 108]], 28],
  [18, 89, 718, [[5, 120], [1, 121]], 30],
  [19, 93, 792, [[3, 113], [4, 114]], 28],
  [20, 97, 858, [[3, 107], [5, 108]], 28],
  [21, 101, 929, [[4, 116], [4, 117]], 28],
  [22, 105, 1003, [[2, 111], [7, 112]], 28],
  [23, 109, 1091, [[4, 121], [5, 122]], 30],
  [24, 113, 1171, [[6, 117], [4, 118]], 30],
  [25, 117, 1273, [[8, 106], [4, 107]], 26],
  [26, 121, 1347, [[10, 114], [2, 115]], 28],
  [27, 125, 1425, [[8, 122], [4, 123]], 30],
  [28, 129, 1501, [[3, 117], [10, 118]], 30],
  [29, 133, 1581, [[7, 116], [7, 117]], 30],
  [30, 137, 1677, [[5, 115], [10, 116]], 30],
  [31, 141, 1782, [[13, 115], [3, 116]], 30],
  [32, 145, 1897, [[17, 115]], 30],
  [33, 149, 2022, [[17, 115], [1, 116]], 30],
  [34, 153, 2157, [[19, 113], [1, 114]], 30],
  [35, 157, 2301, [[18, 107], [4, 108]], 30],
  [36, 161, 2431, [[22, 110], [1, 111]], 30],
  [37, 165, 2561, [[21, 111], [2, 112]], 30],
  [38, 169, 2711, [[19, 112], [5, 113]], 30],
  [39, 173, 2871, [[22, 114], [4, 115]], 30],
  [40, 177, 3031, [[22, 112], [7, 113]], 30]
];

const QR_ALIGN_POS = [
  null,
  [], [6, 18], [6, 22], [6, 26], [6, 30], [6, 34],
  [6, 22, 38], [6, 24, 42], [6, 26, 46], [6, 28, 50],
  [6, 30, 54], [6, 32, 58], [6, 34, 62], [6, 26, 46, 66],
  [6, 26, 48, 70], [6, 26, 50, 74], [6, 30, 54, 78], [6, 30, 56, 82],
  [6, 30, 58, 86], [6, 34, 62, 90], [6, 28, 50, 72, 94], [6, 26, 50, 74, 98],
  [6, 30, 54, 78, 102], [6, 28, 54, 80, 106], [6, 32, 58, 84, 110], [6, 30, 58, 86, 114],
  [6, 34, 62, 90, 118], [6, 26, 50, 74, 98, 122], [6, 30, 54, 78, 102, 126], [6, 26, 52, 78, 104, 130],
  [6, 30, 56, 82, 108, 134], [6, 34, 60, 86, 112, 138], [6, 30, 58, 86, 114, 142], [6, 34, 62, 90, 118, 146],
  [6, 30, 54, 78, 102, 126, 150], [6, 24, 50, 76, 102, 128, 154], [6, 28, 54, 80, 106, 132, 158], [6, 32, 58, 84, 110, 136, 162],
  [6, 26, 54, 82, 110, 138, 166], [6, 30, 58, 86, 114, 142, 170]
];

function getFormatBits(ecLevel, mask) {
  const data = (ecLevel << 3) | mask;
  let rem = data << 10;
  const g = 0x537;
  for (let i = 4; i >= 0; i--) {
    if (rem & (1 << (i + 10))) {
      rem ^= g << i;
    }
  }
  return ((data << 10) | rem) ^ 0x5412;
}

function getVersionBits(version) {
  let rem = version << 12;
  const g = 0x1f25;
  for (let i = 5; i >= 0; i--) {
    if (rem & (1 << (i + 12))) {
      rem ^= g << i;
    }
  }
  return (version << 12) | rem;
}

function generateQRCodeSVG(text) {
  const utf8Encoder = new (typeof TextEncoder !== 'undefined' ? TextEncoder : require('util').TextEncoder)();
  const textBytes = utf8Encoder.encode(text);
  
  let ver = 1;
  while (ver <= 40) {
    const spec = QR_VERSIONS_L[ver];
    const totalData = spec[2];
    const charCountBits = ver >= 10 ? 16 : 8;
    const headerBits = 4 + charCountBits;
    const availableBytes = Math.floor((totalData * 8 - headerBits) / 8);
    if (textBytes.length <= availableBytes) break;
    ver++;
  }
  if (ver > 40) throw new Error('Text too long for QR code');

  const spec = QR_VERSIONS_L[ver];
  const verNumber = spec[0];
  const size = spec[1];
  const maxDataBytes = spec[2];
  const blockSpecs = spec[3];
  const ecBytesPerBlock = spec[4];

  const bitBuf = [];
  function pushBits(val, count) {
    for (let i = count - 1; i >= 0; i--) {
      bitBuf.push((val >> i) & 1);
    }
  }

  pushBits(4, 4);
  const countBits = verNumber >= 10 ? 16 : 8;
  pushBits(textBytes.length, countBits);
  for (let i = 0; i < textBytes.length; i++) {
    pushBits(textBytes[i], 8);
  }

  const maxBits = maxDataBytes * 8;
  const termBits = Math.min(4, maxBits - bitBuf.length);
  if (termBits > 0) pushBits(0, termBits);

  while (bitBuf.length % 8 !== 0) bitBuf.push(0);

  const padBytes = [0xec, 0x11];
  let padIdx = 0;
  while (bitBuf.length < maxBits) {
    pushBits(padBytes[padIdx], 8);
    padIdx = (padIdx + 1) % 2;
  }

  const dataBytes = new Uint8Array(maxDataBytes);
  for (let i = 0; i < maxDataBytes; i++) {
    let b = 0;
    for (let j = 0; j < 8; j++) {
      b = (b << 1) | bitBuf[i * 8 + j];
    }
    dataBytes[i] = b;
  }

  const dataBlocks = [];
  const ecBlocks = [];
  let byteOffset = 0;
  for (let s = 0; s < blockSpecs.length; s++) {
    const numBlocks = blockSpecs[s][0];
    const blockLen = blockSpecs[s][1];
    for (let b = 0; b < numBlocks; b++) {
      const bData = dataBytes.slice(byteOffset, byteOffset + blockLen);
      byteOffset += blockLen;
      const bEc = rsRemainder(bData, ecBytesPerBlock);
      dataBlocks.push(bData);
      ecBlocks.push(bEc);
    }
  }

  const finalCodewords = [];
  let maxBlockLen = 0;
  for (let i = 0; i < dataBlocks.length; i++) {
    if (dataBlocks[i].length > maxBlockLen) maxBlockLen = dataBlocks[i].length;
  }
  for (let i = 0; i < maxBlockLen; i++) {
    for (let b = 0; b < dataBlocks.length; b++) {
      if (i < dataBlocks[b].length) {
        finalCodewords.push(dataBlocks[b][i]);
      }
    }
  }

  for (let i = 0; i < ecBytesPerBlock; i++) {
    for (let b = 0; b < ecBlocks.length; b++) {
      finalCodewords.push(ecBlocks[b][i]);
    }
  }

  const finalBits = [];
  for (let i = 0; i < finalCodewords.length; i++) {
    for (let j = 7; j >= 0; j--) {
      finalBits.push((finalCodewords[i] >> j) & 1);
    }
  }

  const grid = Array.from({ length: size }, () => new Uint8Array(size));
  const reserved = Array.from({ length: size }, () => new Uint8Array(size));

  function setModule(r, c, isDark, isRes = true) {
    grid[r][c] = isDark ? 1 : 2;
    if (isRes) reserved[r][c] = 1;
  }

  function drawFinder(r0, c0) {
    for (let r = -1; r <= 7; r++) {
      for (let c = -1; c <= 7; c++) {
        const rr = r0 + r;
        const cc = c0 + c;
        if (rr >= 0 && rr < size && cc >= 0 && cc < size) {
          const isDark = (r >= 0 && r <= 6 && (r === 0 || r === 6 || c === 0 || c === 6 || (r >= 2 && r <= 4 && c >= 2 && c <= 4)));
          setModule(rr, cc, isDark);
        }
      }
    }
  }

  drawFinder(0, 0);
  drawFinder(0, size - 7);
  drawFinder(size - 7, 0);

  for (let i = 8; i < size - 8; i++) {
    if (!reserved[6][i]) setModule(6, i, i % 2 === 0);
    if (!reserved[i][6]) setModule(i, 6, i % 2 === 0);
  }

  const alignCoords = QR_ALIGN_POS[verNumber];
  for (let i = 0; i < alignCoords.length; i++) {
    for (let j = 0; j < alignCoords.length; j++) {
      const r0 = alignCoords[i];
      const c0 = alignCoords[j];
      let overlap = false;
      for (let dr = -2; dr <= 2; dr++) {
        for (let dc = -2; dc <= 2; dc++) {
          if (reserved[r0 + dr] && reserved[r0 + dr][c0 + dc]) overlap = true;
        }
      }
      if (!overlap) {
        for (let dr = -2; dr <= 2; dr++) {
          for (let dc = -2; dc <= 2; dc++) {
            const isDark = Math.max(Math.abs(dr), Math.abs(dc)) !== 1;
            setModule(r0 + dr, c0 + dc, isDark);
          }
        }
      }
    }
  }

  setModule(4 * verNumber + 9, 8, true);

  for (let i = 0; i < 9; i++) {
    if (!reserved[8][i]) reserved[8][i] = 1;
    if (!reserved[i][8]) reserved[i][8] = 1;
    if (!reserved[8][size - 1 - i]) reserved[8][size - 1 - i] = 1;
    if (!reserved[size - 1 - i][8]) reserved[size - 1 - i][8] = 1;
  }

  if (verNumber >= 7) {
    for (let r = 0; r < 6; r++) {
      for (let c = 0; c < 3; c++) {
        reserved[r][size - 11 + c] = 1;
        reserved[size - 11 + c][r] = 1;
      }
    }
  }

  let bitIdx = 0;
  let dir = -1;
  let col = size - 1;
  while (col > 0) {
    if (col === 6) col--;
    const rowStart = dir === -1 ? size - 1 : 0;
    const rowEnd = dir === -1 ? -1 : size;
    for (let r = rowStart; r !== rowEnd; r += dir) {
      for (let c = col; c >= col - 1; c--) {
        if (!reserved[r][c]) {
          const isDark = bitIdx < finalBits.length ? finalBits[bitIdx++] === 1 : false;
          grid[r][c] = isDark ? 1 : 2;
        }
      }
    }
    dir = -dir;
    col -= 2;
  }

  const mask = 0;
  function isMasked(r, c, pattern) {
    switch (pattern) {
      case 0: return (r + c) % 2 === 0;
      case 1: return r % 2 === 0;
      case 2: return c % 3 === 0;
      case 3: return (r + c) % 3 === 0;
      case 4: return (Math.floor(r / 2) + Math.floor(c / 3)) % 2 === 0;
      case 5: return ((r * c) % 2 + (r * c) % 3) === 0;
      case 6: return (((r * c) % 2 + (r * c) % 3) % 2) === 0;
      case 7: return (((r + c) % 2 + (r * c) % 3) % 2) === 0;
    }
    return false;
  }

  for (let r = 0; r < size; r++) {
    for (let c = 0; c < size; c++) {
      if (!reserved[r][c]) {
        if (isMasked(r, c, mask)) {
          grid[r][c] = grid[r][c] === 1 ? 2 : 1;
        }
      }
    }
  }

  const formatBits = getFormatBits(1, mask);
  const fmtPos1 = [
    [8, 0], [8, 1], [8, 2], [8, 3], [8, 4], [8, 5], [8, 7], [8, 8],
    [7, 8], [5, 8], [4, 8], [3, 8], [2, 8], [1, 8], [0, 8]
  ];
  const fmtPos2 = [
    [size - 1, 8], [size - 2, 8], [size - 3, 8], [size - 4, 8], [size - 5, 8], [size - 6, 8], [size - 7, 8],
    [8, size - 8], [8, size - 7], [8, size - 6], [8, size - 5], [8, size - 4], [8, size - 3], [8, size - 2], [8, size - 1]
  ];
  for (let i = 0; i < 15; i++) {
    const bit = (formatBits >> i) & 1;
    grid[fmtPos1[i][0]][fmtPos1[i][1]] = bit ? 1 : 2;
    grid[fmtPos2[i][0]][fmtPos2[i][1]] = bit ? 1 : 2;
  }

  if (verNumber >= 7) {
    const verBits = getVersionBits(verNumber);
    for (let i = 0; i < 18; i++) {
      const bit = (verBits >> i) & 1;
      const r = Math.floor(i / 3);
      const c = (i % 3);
      grid[r][size - 11 + c] = bit ? 1 : 2;
      grid[size - 11 + c][r] = bit ? 1 : 2;
    }
  }

  const quietZone = 4;
  const viewBoxSize = size + quietZone * 2;
  let pathD = "";
  for (let r = 0; r < size; r++) {
    for (let c = 0; c < size; c++) {
      if (grid[r][c] === 1) {
        pathD += `M${c + quietZone},${r + quietZone}h1v1h-1z`;
      }
    }
  }

  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${viewBoxSize} ${viewBoxSize}" width="100%" height="100%" shape-rendering="crispEdges"><rect width="100%" height="100%" fill="#ffffff"/><path fill="#000000" d="${pathD}"/></svg>`;
}

function generateQRCodeDataURL(text) {
  const svg = generateQRCodeSVG(text);
  return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
}

function waitForDataChannelBuffer(dc, highWatermark = 1024 * 1024, lowWatermark = 512 * 1024, pollMs = 25) {
  const targetThreshold = (typeof lowWatermark === 'number') ? lowWatermark : highWatermark;
  return waitForBufferedAmountLow(dc, targetThreshold, pollMs);
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    waitForBufferedAmountLow,
    waitForDataChannelBuffer,
    uploadFileP2P,
    sendWebRTCFile,
    generateQRCodeSVG,
    generateQRCodeDataURL,
    generateClientQRCodeDataURL,
    SequentialChunkQueue,
    WebRTCStreamDecrypter,
    decompressOffer,
    setState,
    renderFileCard,
    updateProgress,
    VirtualLogViewer,
    startHTTPSSE,
    getBackendURL,
    apiPath,
    formatBytes,
    mimeLabel,
    resetState,
    stripAnsi,
    parseAnsiToHtml,
    handleSenderFileSelect,
    startSenderSharing,
    get_senderEncryptionKey: () => senderEncryptionKey,
    OPFSStreamWriter,
    createOPFSWriter,
    checkRamWarning,
    extractKeyFragment,
    parseDecryptionKeyFromHash,
    parseSessionInput,
    getIceServers
  };
}
